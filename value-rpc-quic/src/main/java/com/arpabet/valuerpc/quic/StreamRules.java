/*
 * Copyright (c) 2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package com.arpabet.valuerpc.quic;

import com.arpabet.valuerpc.MessageType;

/**
 * How requests map to QUIC streams: who opens the stream of a request, and
 * when each side has sent the last it will send on one, so that it can end
 * its half of the stream. Both ends have to agree on this, or streams are
 * never freed; it is the Go implementation's {@code quic/transport.go}, as
 * of value-rpc v1.7.2.
 *
 * <p>A request has a stream of its own. The side that makes the request opens
 * it with the message that opens the request; every later message of that
 * request, in either direction, travels on it. A stream is over when both
 * sides have ended their half.
 */
final class StreamRules {

    private StreamRules() {
    }

    /**
     * Whether this side is the one that opens the stream of a request with
     * this id. The client owns the handshake, which is 0, and its positive
     * ids; the server owns the negative ones, of the requests it makes of a
     * client.
     */
    static boolean originates(boolean client, long requestId) {
        return client ? requestId >= 0 : requestId < 0;
    }

    /**
     * Whether a message this side sends is the last it will send on the
     * stream, as the Go implementation decides it ({@code finOnWrite}).
     *
     * <p>The side that made the request is not done when it has sent it: of
     * a stream from the peer it still sends credit, and may cancel. It is
     * done with the end of what it streams, or with a cancellation; otherwise
     * it ends its half when it reads the answer that ends the request (see
     * {@link #goFinOnRead}). The side that answers is done with the answer
     * that ends the request. Of a stream to it, it keeps sending credit after
     * it said it is ready, so that does not end its half: reading the end of
     * the stream does.
     *
     * <p>A chat is both at once for the side that opened it: it streams to
     * the peer, and the peer streams to it. The end of what it streams does
     * not end its half. The chat goes on after that: the peer still streams,
     * until it ends the chat, and has to be granted credit for what it sends.
     * With its half ended this side could grant none, and the peer would stop
     * after the values it had credit for, for good. The Go implementation
     * did end the half there up to value-rpc v1.7.1, and a chat it had opened
     * and ended first stopped that way.
     *
     * @param originatedHere whether this side made the request
     * @param sent           the message it sends
     * @param request        the message that opened the request
     */
    static boolean goFinOnWrite(boolean originatedHere, MessageType sent, MessageType request) {
        if (originatedHere) {
            if (request == MessageType.CHAT_REQUEST && sent == MessageType.STREAM_END) {
                return false;
            }
            return sent == MessageType.STREAM_END || sent == MessageType.CANCEL_REQUEST;
        }
        return sent == MessageType.HANDSHAKE_RESPONSE || sent == MessageType.FUNCTION_RESPONSE
                || sent == MessageType.ERROR_RESPONSE || sent == MessageType.STREAM_END;
    }

    /**
     * Whether a message this side reads means that it will send no more on
     * the stream, as the Go implementation decides it ({@code finOnRead}).
     *
     * <p>For the side that opened a chat that is the message the peer ends
     * the chat with: the end of what the peer streams, whether or not this
     * side had sent its own, or the failure the peer refuses or fails it with.
     *
     * @param received       the message it read
     * @param request        the message that opened the request
     * @param originatedHere whether this side made the request
     */
    static boolean goFinOnRead(MessageType received, MessageType request, boolean originatedHere) {
        if (originatedHere) {
            if (request == MessageType.FUNCTION_REQUEST) {
                return received == MessageType.FUNCTION_RESPONSE || received == MessageType.ERROR_RESPONSE;
            }
            if (request == MessageType.GET_STREAM_REQUEST || request == MessageType.CHAT_REQUEST) {
                return received == MessageType.STREAM_END || received == MessageType.ERROR_RESPONSE;
            }
            if (request == MessageType.HANDSHAKE_REQUEST) {
                return received == MessageType.HANDSHAKE_RESPONSE;
            }
            return false;
        }
        // The side that answers: only of a stream to it does it keep sending,
        // credit, after it said it is ready.
        if (request == MessageType.PUT_STREAM_REQUEST) {
            return received == MessageType.STREAM_END || received == MessageType.ERROR_RESPONSE
                    || received == MessageType.CANCEL_REQUEST;
        }
        return false;
    }

    /**
     * Whether a message is one after which its request is over for both
     * sides, whoever sends it: a failure, or a cancellation, which is not
     * answered.
     */
    private static boolean endsTheRequest(MessageType message) {
        return message == MessageType.ERROR_RESPONSE || message == MessageType.CANCEL_REQUEST;
    }

    /**
     * Whether to end this side's half of the stream after sending a message.
     * It is what the Go implementation does, and one thing more.
     *
     * <p>A half is also ended after any message that ends the request for
     * both sides. The Go implementation leaves it open in a few of those
     * cases, where nothing more will be sent: the stream then lasts as long
     * as the connection, on both ends. Ending a half early is safe towards
     * it: it reads the end of the stream as the peer being done, which is the
     * truth.
     *
     * @param originatedHere whether this side made the request
     * @param sent           the message it sends
     * @param request        the message that opened the request
     */
    static boolean finOnWrite(boolean originatedHere, MessageType sent, MessageType request) {
        return endsTheRequest(sent) || goFinOnWrite(originatedHere, sent, request);
    }

    /**
     * Whether to end this side's half of the stream after reading a message:
     * when the Go implementation would, and after any message that ends the
     * request. See {@link #finOnWrite}.
     */
    static boolean finOnRead(MessageType received, MessageType request, boolean originatedHere) {
        return endsTheRequest(received) || goFinOnRead(received, request, originatedHere);
    }
}
