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
 * never freed; it is the Go implementation's {@code quic/transport.go}.
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
     * @param originatedHere whether this side made the request
     * @param sent           the message it sends
     */
    static boolean goFinOnWrite(boolean originatedHere, MessageType sent) {
        if (originatedHere) {
            return sent == MessageType.STREAM_END || sent == MessageType.CANCEL_REQUEST;
        }
        return sent == MessageType.HANDSHAKE_RESPONSE || sent == MessageType.FUNCTION_RESPONSE
                || sent == MessageType.ERROR_RESPONSE || sent == MessageType.STREAM_END;
    }

    /**
     * Whether a message this side reads means that it will send no more on
     * the stream, as the Go implementation decides it ({@code finOnRead}).
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
            if (request == MessageType.GET_STREAM_REQUEST) {
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
     * It is what the Go implementation does, but for two things.
     *
     * <p>A half is also ended after any message that ends the request for
     * both sides. The Go implementation leaves it open in a few of those
     * cases, where nothing more will be sent: the stream then lasts as long
     * as the connection, on both ends. Ending a half early is safe towards
     * it: it reads the end of the stream as the peer being done, which is the
     * truth.
     *
     * <p>And the side that opened a chat does not end its half with the end
     * of what it streams, as the Go implementation does. A chat goes on after
     * that: the peer still streams, until it ends the chat, and has to be
     * granted credit for what it sends. With its half ended this side could
     * grant none, and the peer would stop after the values it had credit for,
     * for good. The half is ended when the peer ends the chat (see
     * {@link #finOnRead}).
     *
     * @param originatedHere whether this side made the request
     * @param sent           the message it sends
     * @param request        the message that opened the request
     */
    static boolean finOnWrite(boolean originatedHere, MessageType sent, MessageType request) {
        if (endsTheRequest(sent)) {
            return true;
        }
        if (originatedHere && request == MessageType.CHAT_REQUEST && sent == MessageType.STREAM_END) {
            return false;
        }
        return goFinOnWrite(originatedHere, sent);
    }

    /**
     * Whether to end this side's half of the stream after reading a message:
     * when the Go implementation would, after any message that ends the
     * request, and, for the side that opened a chat, when the peer ends what
     * it streams, which ends the chat: whether or not this side had sent its
     * last, it sends nothing after that. See {@link #finOnWrite}.
     */
    static boolean finOnRead(MessageType received, MessageType request, boolean originatedHere) {
        if (endsTheRequest(received)) {
            return true;
        }
        if (originatedHere && request == MessageType.CHAT_REQUEST && received == MessageType.STREAM_END) {
            return true;
        }
        return goFinOnRead(received, request, originatedHere);
    }
}
