/*
 * Copyright (c) 2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package com.arpabet.valuerpc.quic;

import static com.arpabet.valuerpc.MessageType.CANCEL_REQUEST;
import static com.arpabet.valuerpc.MessageType.CHAT_REQUEST;
import static com.arpabet.valuerpc.MessageType.ERROR_RESPONSE;
import static com.arpabet.valuerpc.MessageType.FUNCTION_REQUEST;
import static com.arpabet.valuerpc.MessageType.FUNCTION_RESPONSE;
import static com.arpabet.valuerpc.MessageType.GET_STREAM_REQUEST;
import static com.arpabet.valuerpc.MessageType.HANDSHAKE_REQUEST;
import static com.arpabet.valuerpc.MessageType.HANDSHAKE_RESPONSE;
import static com.arpabet.valuerpc.MessageType.PUT_STREAM_REQUEST;
import static com.arpabet.valuerpc.MessageType.STREAM_CREDIT;
import static com.arpabet.valuerpc.MessageType.STREAM_END;
import static com.arpabet.valuerpc.MessageType.STREAM_READY;
import static com.arpabet.valuerpc.MessageType.STREAM_VALUE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.arpabet.valuerpc.MessageType;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * Who opens a stream and when each side ends its half of one: the rules of
 * the Go implementation ({@code finOnWrite}, {@code finOnRead} and
 * {@code streamForWrite} of {@code quic/transport.go}), case by case, and
 * what this implementation adds to them.
 */
class StreamRulesTest {

    private static final MessageType[] REQUESTS =
            {HANDSHAKE_REQUEST, FUNCTION_REQUEST, GET_STREAM_REQUEST, PUT_STREAM_REQUEST, CHAT_REQUEST};

    private static Set<MessageType> of(MessageType... types) {
        return types.length == 0 ? EnumSet.noneOf(MessageType.class) : EnumSet.copyOf(Arrays.asList(types));
    }

    @Test
    void theClientOpensTheHandshakeAndItsPositiveIdsTheServerTheNegativeOnes() {
        assertTrue(StreamRules.originates(true, 0), "the handshake is the client's");
        assertTrue(StreamRules.originates(true, 1));
        assertTrue(StreamRules.originates(true, Long.MAX_VALUE));
        assertFalse(StreamRules.originates(true, -1));
        assertFalse(StreamRules.originates(false, 0));
        assertFalse(StreamRules.originates(false, 1));
        assertTrue(StreamRules.originates(false, -1));
        assertTrue(StreamRules.originates(false, Long.MIN_VALUE));
    }

    @Test
    void whatTheSideThatMadeTheRequestSendsLastAsInGo() {
        // Its own end of what it streams, or its cancellation. Not the
        // request: of a stream from the peer it still sends credit.
        final Set<MessageType> last = of(STREAM_END, CANCEL_REQUEST);
        for (MessageType sent : MessageType.values()) {
            assertEquals(last.contains(sent), StreamRules.goFinOnWrite(true, sent), "sending " + sent);
        }
    }

    @Test
    void whatTheSideThatAnswersSendsLastAsInGo() {
        // The answer that ends the request. Not that it is ready: of a
        // stream to it, it still sends credit after that.
        final Set<MessageType> last = of(HANDSHAKE_RESPONSE, FUNCTION_RESPONSE, ERROR_RESPONSE, STREAM_END);
        for (MessageType sent : MessageType.values()) {
            assertEquals(last.contains(sent), StreamRules.goFinOnWrite(false, sent), "sending " + sent);
        }
    }

    @Test
    void whatTheSideThatMadeTheRequestReadsLastAsInGo() {
        for (MessageType received : MessageType.values()) {
            assertEquals(received == HANDSHAKE_RESPONSE,
                    StreamRules.goFinOnRead(received, HANDSHAKE_REQUEST, true), "a handshake, reading " + received);
            assertEquals(of(FUNCTION_RESPONSE, ERROR_RESPONSE).contains(received),
                    StreamRules.goFinOnRead(received, FUNCTION_REQUEST, true), "a call, reading " + received);
            assertEquals(of(STREAM_END, ERROR_RESPONSE).contains(received),
                    StreamRules.goFinOnRead(received, GET_STREAM_REQUEST, true), "a stream from the peer, reading " + received);
            // Of what it streams itself it is done when it sends its end.
            assertFalse(StreamRules.goFinOnRead(received, PUT_STREAM_REQUEST, true), "a stream to the peer, reading " + received);
            assertFalse(StreamRules.goFinOnRead(received, CHAT_REQUEST, true), "a chat, reading " + received);
        }
    }

    @Test
    void whatTheSideThatAnswersReadsLastAsInGo() {
        for (MessageType received : MessageType.values()) {
            // Only of a stream to it does it keep sending, credit, until the stream ends.
            assertEquals(of(STREAM_END, ERROR_RESPONSE, CANCEL_REQUEST).contains(received),
                    StreamRules.goFinOnRead(received, PUT_STREAM_REQUEST, false), "a stream to it, reading " + received);
            for (MessageType request : new MessageType[] {HANDSHAKE_REQUEST, FUNCTION_REQUEST, GET_STREAM_REQUEST, CHAT_REQUEST}) {
                assertFalse(StreamRules.goFinOnRead(received, request, false), request + ", reading " + received);
            }
        }
    }

    @Test
    void aRequestOfAnUnknownKindEndsNoHalfOnReading() {
        for (MessageType received : new MessageType[] {STREAM_READY, STREAM_VALUE, STREAM_END, STREAM_CREDIT, FUNCTION_RESPONSE}) {
            assertFalse(StreamRules.goFinOnRead(received, null, true));
            assertFalse(StreamRules.goFinOnRead(received, null, false));
        }
    }

    private static final boolean[] EITHER = {true, false};

    /** The one case in which this implementation does otherwise than the Go one where that one ends a half. */
    private static boolean endOfAChatByWhoOpenedIt(boolean originatedHere, MessageType message, MessageType request) {
        return originatedHere && request == CHAT_REQUEST && message == STREAM_END;
    }

    @Test
    void whereTheGoImplementationEndsAHalfThisOneDoesTooButForWhoOpenedAChat() {
        for (MessageType message : MessageType.values()) {
            for (boolean originatedHere : EITHER) {
                for (MessageType request : REQUESTS) {
                    if (StreamRules.goFinOnWrite(originatedHere, message)
                            && !endOfAChatByWhoOpenedIt(originatedHere, message, request)) {
                        assertTrue(StreamRules.finOnWrite(originatedHere, message, request),
                                "sending " + message + " of " + request);
                    }
                    if (StreamRules.goFinOnRead(message, request, originatedHere)) {
                        assertTrue(StreamRules.finOnRead(message, request, originatedHere),
                                "reading " + message + " of " + request);
                    }
                }
            }
        }
    }

    @Test
    void aFailureOrACancellationEndsTheHalfOfWhoeverSendsOrReadsIt() {
        // More than the Go implementation: after either, the request is over
        // for both sides, whatever it was and whoever made it.
        for (MessageType over : new MessageType[] {ERROR_RESPONSE, CANCEL_REQUEST}) {
            for (boolean originatedHere : EITHER) {
                for (MessageType request : REQUESTS) {
                    assertTrue(StreamRules.finOnWrite(originatedHere, over, request));
                    assertTrue(StreamRules.finOnRead(over, request, originatedHere));
                }
                assertTrue(StreamRules.finOnWrite(originatedHere, over, null));
                assertTrue(StreamRules.finOnRead(over, null, originatedHere));
            }
        }
    }

    @Test
    void whoOpenedAChatKeepsItsHalfUntilThePeerEndsTheChat() {
        // Otherwise than the Go implementation, which ends the half with the
        // end of what that side streams. A chat goes on after that: the peer
        // still streams, and has to be granted credit for it.
        assertTrue(StreamRules.goFinOnWrite(true, STREAM_END), "the Go implementation ends the half");
        assertFalse(StreamRules.finOnWrite(true, STREAM_END, CHAT_REQUEST), "the half stays, for the credit of the peer");
        // The peer ending what it streams ends the chat: nothing is sent after it.
        assertFalse(StreamRules.goFinOnRead(STREAM_END, CHAT_REQUEST, true), "the Go implementation leaves the half open");
        assertTrue(StreamRules.finOnRead(STREAM_END, CHAT_REQUEST, true));
        // What travels in between ends nothing.
        for (MessageType between : new MessageType[] {STREAM_READY, STREAM_VALUE, STREAM_CREDIT}) {
            assertFalse(StreamRules.finOnWrite(true, between, CHAT_REQUEST));
            assertFalse(StreamRules.finOnRead(between, CHAT_REQUEST, true));
        }
    }

    @Test
    void whoAnswersAChatEndsItWithTheEndOfWhatItStreamsAsInGo() {
        // The end of what the one who opened it streams does not end the chat.
        assertFalse(StreamRules.finOnRead(STREAM_END, CHAT_REQUEST, false));
        // Its own does, and its half with it.
        assertTrue(StreamRules.finOnWrite(false, STREAM_END, CHAT_REQUEST));
        for (MessageType between : new MessageType[] {STREAM_READY, STREAM_VALUE, STREAM_CREDIT}) {
            assertFalse(StreamRules.finOnWrite(false, between, CHAT_REQUEST));
            assertFalse(StreamRules.finOnRead(between, CHAT_REQUEST, false));
        }
    }

    @Test
    void nothingElseIsOtherThanInTheGoImplementation() {
        for (MessageType message : MessageType.values()) {
            if (message == ERROR_RESPONSE || message == CANCEL_REQUEST) {
                continue;
            }
            for (boolean originatedHere : EITHER) {
                for (MessageType request : REQUESTS) {
                    if (endOfAChatByWhoOpenedIt(originatedHere, message, request)) {
                        continue;
                    }
                    assertEquals(StreamRules.goFinOnWrite(originatedHere, message),
                            StreamRules.finOnWrite(originatedHere, message, request),
                            "sending " + message + " of " + request);
                    assertEquals(StreamRules.goFinOnRead(message, request, originatedHere),
                            StreamRules.finOnRead(message, request, originatedHere),
                            "reading " + message + " of " + request);
                }
            }
        }
    }
}
