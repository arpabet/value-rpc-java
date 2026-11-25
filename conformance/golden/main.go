/*
 * Copyright (c) 2026 Karagatan LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

// Command golden writes the files the Java implementation is tested against:
// what go.arpabet.com/value-rpc, the reference, gives for the envelopes of the
// protocol, the hash chain, the JSON wire codec and the verification of
// arguments. The Java tests read them from value-rpc/src/test/resources/golden
// (or from the directory named by the Gradle property goldenDir) and must give
// the same answers.
//
//	go run ./golden                       # the committed files
//	go run ./golden -out /tmp/golden -scale 20 -seed 7
//
// Every file is tab-separated; a text is in base64 so that any byte travels.
package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"math"
	"math/big"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"time"

	val "go.arpabet.com/value"
	vrpc "go.arpabet.com/value-rpc/valuerpc"
)

var rng *rand.Rand

func main() {
	out := flag.String("out", "../value-rpc/src/test/resources/golden", "directory to write the golden files to")
	scale := flag.Int("scale", 1, "multiplies the size of the random parts")
	seed := flag.Int64("seed", 20261124, "seed of the random parts")
	flag.Parse()
	rng = rand.New(rand.NewSource(*seed))
	if err := os.MkdirAll(*out, 0o755); err != nil {
		panic(err)
	}
	write(*out, "frames.tsv", framesFile())
	write(*out, "codes.tsv", codesFile())
	write(*out, "hashchain.tsv", hashChainFile(*scale))
	write(*out, "verify.tsv", verifyFile())
	write(*out, "json.tsv", jsonFile(*scale))
}

func write(dir, name string, lines []string) {
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o644); err != nil {
		panic(err)
	}
	fmt.Printf("%s: %d lines\n", path, len(lines))
}

func b64(s string) string { return base64.StdEncoding.EncodeToString([]byte(s)) }

// u is the JSON escape of a UTF-16 code unit as it stands in a text: a
// backslash, u and four hexadecimal digits. upper has the digits in upper case.
func u(unit int) string     { return fmt.Sprintf("%cu%04x", '\\', unit) }
func upper(unit int) string { return fmt.Sprintf("%cu%04X", '\\', unit) }

// q is a JSON string of the parts.
func q(parts ...string) string { return `"` + strings.Join(parts, "") + `"` }

func packed(v val.Value) string {
	if v == nil {
		return "nil"
	}
	b, err := val.Pack(v)
	if err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}

// --- frames.tsv ---------------------------------------------------------------
//
// envelope <name> <msgpack hex> <base64 of the JSON>
// frame    <name> <hex of the frame on a stream: length and bytes>
// batch    <names, comma separated> <hex of the frames written at once>
//
// The Java test builds the envelope of each name itself (EnvelopeGoldenTest) and
// must get these bytes.

type named struct {
	name string
	msg  val.Map
}

const anchor = "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"

func list(items ...val.Value) val.Value { return val.ImmutableList(items) }

func mapOf(m map[string]val.Value) val.Map { return val.ImmutableMapOf(m) }

// clientRequest is the request a client sends (valueclient.constructRequest and
// sendRequest): the arguments are always there, null when there are none.
func clientRequest(mt vrpc.MessageType, rid int64, name string, args val.Value, sla int64, md vrpc.Metadata) val.Map {
	d := vrpc.DefaultDialect
	if args == nil {
		args = val.Null
	}
	entries := []val.MapEntry{
		val.ImmutableEntry(d.MessageTypeField, mt.Long()),
		val.ImmutableEntry(d.FunctionNameField, val.Utf8(name)),
		val.ImmutableEntry(d.ArgumentsField, args),
	}
	if sla > 0 {
		entries = append(entries, val.ImmutableEntry(d.TimeoutField, val.Long(sla)))
	}
	if m := vrpc.EncodeMetadata(md); m != nil {
		entries = append(entries, val.ImmutableEntry(d.MetadataField, m))
	}
	entries = append(entries, val.ImmutableEntry(d.RequestIdField, val.Long(rid)))
	return val.SortedMap(entries, false)
}

func envelopes() []named {
	d := vrpc.DefaultDialect
	return []named{
		{"handshake-request", vrpc.NewHandshakeRequest(712398211, anchor)},
		{"handshake-request-no-token", vrpc.NewHandshakeRequest(1, "")},
		{"handshake-request-auth", vrpc.NewHandshakeRequest(712398211, anchor).Put(d.AuthField, val.Utf8("secret"))},
		{"handshake-request-auth-map", vrpc.NewHandshakeRequest(42, "").Put(d.AuthField,
			mapOf(map[string]val.Value{"user": val.Utf8("ann"), "key": val.Raw([]byte{1, 2, 3}, true)}))},
		{"handshake-request-auth-null", vrpc.NewHandshakeRequest(42, "").Put(d.AuthField, val.Null)},
		{"handshake-request-max-cid", vrpc.NewHandshakeRequest(math.MaxInt64, anchor)},
		{"handshake-response", vrpc.NewHandshakeResponse("")},
		{"handshake-response-token", vrpc.NewHandshakeResponse("abc")},

		{"request-reverse-no-args", vrpc.NewFunctionRequest(-1, "notify", nil)},
		{"request-reverse-args", vrpc.NewFunctionRequest(-2, "notify", list(val.Utf8("ping")))},
		{"request-reverse-md", vrpc.NewFunctionRequest(-3, "notify", list(val.Long(1))).
			Put(d.MetadataField, vrpc.EncodeMetadata(vrpc.Metadata{"traceparent": "00-t"}))},
		{"request-client", clientRequest(vrpc.FunctionRequest, 1, "greet", list(val.Utf8("world")), 5000, nil)},
		{"request-client-no-args", clientRequest(vrpc.FunctionRequest, 2, "ping", nil, 1000, nil)},
		{"request-client-no-sla", clientRequest(vrpc.FunctionRequest, 3, "ping", val.Null, 0, nil)},
		{"request-client-md", clientRequest(vrpc.FunctionRequest, 4, "md.echo", list(), 250,
			vrpc.Metadata{"traceparent": "00-t", "extra": "x"})},
		{"request-get-stream", clientRequest(vrpc.GetStreamRequest, 5, "count", list(val.Long(25)), 1000, nil)},
		{"request-put-stream", clientRequest(vrpc.PutStreamRequest, 6, "upload", list(val.Utf8("batch")), 1000, nil)},
		{"request-chat", clientRequest(vrpc.ChatRequest, 7, "chat.echo", list(val.Utf8("room-1")), 1000, nil)},
		{"request-reverse-get-stream", vrpc.NewStreamRequest(vrpc.GetStreamRequest, -4, "tail", list(val.Long(4)))},
		{"request-reverse-put-stream", vrpc.NewStreamRequest(vrpc.PutStreamRequest, -5, "ingest", nil)},
		{"request-reverse-chat", vrpc.NewStreamRequest(vrpc.ChatRequest, -6, "talk", val.Null)},

		{"result", vrpc.NewFunctionResult(val.Long(1), val.Utf8("Hello, world!"))},
		{"result-nothing", vrpc.NewFunctionResult(val.Long(2), nil)},
		{"result-null", vrpc.NewFunctionResult(val.Long(3), val.Null)},
		{"result-map", vrpc.NewFunctionResult(val.Long(-1), mapOf(map[string]val.Value{
			"sum": val.Long(55), "count": val.Long(10), "ratio": val.Double(0.5), "raw": val.Raw([]byte{0, 255}, true)}))},
		{"error", vrpc.NewFunctionError(val.Long(3), vrpc.CodeNotFound, "function not found %s", "no.such.fn")},
		{"error-handshake", vrpc.NewFunctionError(val.Long(d.HandshakeRequestId), vrpc.CodeUnauthenticated, "%s", "authentication failed")},
		{"error-handler-coded", vrpc.NewHandlerError(val.Long(4), "function fail",
			vrpc.NewError(vrpc.CodeResourceExhausted, "deliberate failure"))},
		{"error-handler-plain", vrpc.NewHandlerError(val.Long(5), "function boom", errors.New("boom"))},

		{"stream-ready", vrpc.NewStreamReady(val.Long(5))},
		{"stream-value", vrpc.NewStreamValue(val.Long(5), val.Long(1))},
		{"stream-value-nothing", vrpc.NewStreamValue(val.Long(5), nil)},
		{"stream-value-reverse", vrpc.NewStreamValue(val.Long(-4), val.Utf8("t0"))},
		{"stream-end", vrpc.NewStreamEnd(val.Long(5), nil)},
		{"stream-end-value", vrpc.NewStreamEnd(val.Long(5), val.Utf8("last"))},
		{"cancel", vrpc.NewCancelRequest(val.Long(5))},
		{"stream-credit", vrpc.NewStreamCredit(val.Long(5), 4096)},
		{"stream-credit-reverse", vrpc.NewStreamCredit(val.Long(-4), 2048)},
	}
}

// capture is a stream that keeps what is written to it.
type capture struct{ bytes.Buffer }

func (c *capture) Read([]byte) (int, error) { return 0, errors.New("write only") }
func (c *capture) Close() error             { return nil }

func framed(msgs ...val.Map) string {
	c := &capture{}
	conn := vrpc.NewMsgConn(c, 0, 0)
	if err := vrpc.WriteBatch(conn, msgs); err != nil {
		panic(err)
	}
	return hex.EncodeToString(c.Bytes())
}

func framesFile() []string {
	var lines []string
	all := envelopes()
	byName := map[string]val.Map{}
	for _, e := range all {
		byName[e.name] = e.msg
		lines = append(lines, strings.Join([]string{"envelope", e.name, packed(e.msg), b64(val.Jsonify(e.msg))}, "\t"))
	}
	for _, name := range []string{"handshake-request", "result-nothing", "stream-value"} {
		lines = append(lines, strings.Join([]string{"frame", name, framed(byName[name])}, "\t"))
	}
	batch := []string{"stream-ready", "stream-value", "stream-value", "stream-end"}
	msgs := make([]val.Map, len(batch))
	for i, name := range batch {
		msgs[i] = byName[name]
	}
	lines = append(lines, strings.Join([]string{"batch", strings.Join(batch, ","), framed(msgs...)}, "\t"))
	return lines
}

// --- codes.tsv ----------------------------------------------------------------
//
// <number> <name> <base64 of Error() of a failure with that code and the message "boom">

func codesFile() []string {
	var lines []string
	for n := -1; n <= 12; n++ {
		c := vrpc.Code(n)
		lines = append(lines, fmt.Sprintf("%d\t%s\t%s", n, c.String(), b64(vrpc.NewError(c, "boom").Error())))
	}
	return lines
}

// --- hashchain.tsv ------------------------------------------------------------
//
// chain  <seed hex> <n> <anchor> <the links it reveals, in order, comma separated>
// verify <presented> <last verified> <window> <steps; 0 when it does not verify>
// same   <a> <b> <true|false>

func chainOf(seed [32]byte, n int) [][32]byte {
	links := make([][32]byte, n+1)
	links[0] = seed
	for i := 1; i <= n; i++ {
		links[i] = sha256.Sum256(links[i-1][:])
	}
	return links
}

func hx(l [32]byte) string { return hex.EncodeToString(l[:]) }

func hashChainFile(scale int) []string {
	var lines []string
	verify := func(presented, last string, window int) {
		steps, ok := vrpc.VerifyHashStep(presented, last, window)
		if !ok {
			steps = 0
		}
		lines = append(lines, fmt.Sprintf("verify\t%s\t%s\t%d\t%d", presented, last, window, steps))
	}
	same := func(a, b string) {
		lines = append(lines, fmt.Sprintf("same\t%s\t%s\t%v", a, b, vrpc.SameToken(a, b)))
	}
	for c := 0; c < 3*scale; c++ {
		var seed [32]byte
		rng.Read(seed[:])
		n := []int{1, 5, 40}[c%3]
		links := chainOf(seed, n)
		var revealed []string
		for i := n - 1; i >= 0; i-- {
			revealed = append(revealed, hx(links[i]))
		}
		lines = append(lines, fmt.Sprintf("chain\t%s\t%d\t%s\t%s", hx(seed), n, hx(links[n]), strings.Join(revealed, ",")))
	}

	var seed [32]byte
	rng.Read(seed[:])
	const n = 1100
	links := chainOf(seed, n)
	last := hx(links[n])
	verify(hx(links[n-1]), last, 0)                  // the next link
	verify(hx(links[n-3]), last, 0)                  // two handshakes were lost
	verify(hx(links[n-1024]), last, 0)               // at the edge of the window
	verify(hx(links[n-1025]), last, 0)               // beyond it
	verify(hx(links[n-3]), last, 2)                  // a window of its own, too short
	verify(hx(links[n-3]), last, 3)                  // and just enough
	verify(last, last, 0)                            // the link it holds is not a next one
	verify(hx(links[n-1]), hx(links[n-2]), 0)        // a link already passed
	verify(strings.ToUpper(hx(links[n-1])), last, 0) // hexadecimal of either case
	verify(hx(links[n-1]), strings.ToUpper(last), 0)
	verify("", last, 0)
	verify(hx(links[n-1]), "", 0)
	verify(hx(links[n-1])[:62], last, 0)
	verify("zz"+hx(links[n-1])[2:], last, 0)
	verify(hx(links[n-1])+"00", last, 0)
	var forged [32]byte
	rng.Read(forged[:])
	verify(hx(forged), last, 0)

	same(last, last)
	same(last, strings.ToUpper(last))
	same(last, hx(links[n-1]))
	same("", "")
	same(last, "")
	same(last[:62], last[:62])
	same("zz"+last[2:], "zz"+last[2:])
	return lines
}

// --- verify.tsv ---------------------------------------------------------------
//
// <type definition> <msgpack hex of the value, or nil for no value> <true|false>
//
// A type definition is any, void, arg:KIND:req|opt, list:KIND:req|opt,... or
// map:name=KIND:req|opt,...

type typed struct {
	spec string
	def  vrpc.TypeDef
}

func verifyFile() []string {
	defs := []typed{
		{"any", vrpc.Any},
		{"void", vrpc.Void},
		{"arg:BOOL:req", vrpc.Bool},
		{"arg:BOOL:opt", vrpc.BoolOpt},
		{"arg:NUMBER:req", vrpc.Number},
		{"arg:NUMBER:opt", vrpc.NumberOpt},
		{"arg:STRING:req", vrpc.String},
		{"arg:STRING:opt", vrpc.StringOpt},
		{"arg:LIST:req", vrpc.Arg(val.LIST, true)},
		{"arg:MAP:opt", vrpc.Arg(val.MAP, false)},
		{"list:", vrpc.List()},
		{"list:STRING:req", vrpc.List(vrpc.String)},
		{"list:STRING:req,NUMBER:opt", vrpc.List(vrpc.String, vrpc.NumberOpt)},
		{"list:NUMBER:opt,BOOL:req", vrpc.List(vrpc.NumberOpt, vrpc.Bool)},
		{"map:", vrpc.Map()},
		{"map:id=NUMBER:req", vrpc.Map(vrpc.Param("id", val.NUMBER, true))},
		{"map:id=NUMBER:req,name=STRING:opt", vrpc.Map(vrpc.Param("id", val.NUMBER, true), vrpc.Param("name", val.STRING, false))},
		{"map:tags=LIST:opt", vrpc.Map(vrpc.Param("tags", val.LIST, false))},
	}
	sparse := val.EmptySparseList().PutAt(0, val.Utf8("a")).PutAt(2, val.Long(1))
	values := []val.Value{
		nil,
		val.Null,
		val.Boolean(true),
		val.Long(0),
		val.Double(1.5),
		val.BigInt(new(big.Int).Lsh(big.NewInt(1), 70)),
		val.Utf8(""),
		val.Utf8("text"),
		val.Raw([]byte{1, 2}, true),
		val.UnixTime(1793345319, 0),
		list(),
		list(val.Utf8("a")),
		list(val.Utf8("a"), val.Long(1)),
		list(val.Utf8("a"), val.Null),
		list(val.Null, val.Boolean(false)),
		list(val.Long(1), val.Boolean(true)),
		list(val.Utf8("a"), val.Long(1), val.Long(2)),
		sparse,
		val.EmptySparseList(),
		mapOf(map[string]val.Value{}),
		mapOf(map[string]val.Value{"id": val.Long(7)}),
		mapOf(map[string]val.Value{"id": val.Utf8("7")}),
		mapOf(map[string]val.Value{"id": val.Long(7), "name": val.Utf8("ann")}),
		mapOf(map[string]val.Value{"id": val.Long(7), "name": val.Long(1)}),
		mapOf(map[string]val.Value{"id": val.Long(7), "name": val.Null}),
		mapOf(map[string]val.Value{"id": val.Null}),
		mapOf(map[string]val.Value{"name": val.Utf8("ann")}),
		mapOf(map[string]val.Value{"id": val.Long(7), "more": val.Boolean(true)}),
		mapOf(map[string]val.Value{"tags": list(val.Utf8("x"))}),
		mapOf(map[string]val.Value{"tags": val.Utf8("x")}),
	}
	// What is verified is the value as it comes off the wire: an empty sparse
	// list, say, has the bytes of the empty map and arrives as one.
	var lines []string
	for _, d := range defs {
		for _, v := range values {
			lines = append(lines, fmt.Sprintf("%s\t%s\t%v", d.spec, packed(v), vrpc.Verify(wire(v), d.def)))
		}
	}
	return lines
}

// wire is a value as its bytes read back.
func wire(v val.Value) val.Value {
	if v == nil {
		return nil
	}
	b, err := val.Pack(v)
	if err != nil {
		panic(err)
	}
	back, err := val.Unpack(b, true)
	if err != nil {
		panic(err)
	}
	return back
}

// --- json.tsv -----------------------------------------------------------------
//
// <base64 of the JSON text> <msgpack hex of what ParseJson reads it as, or error>

func jsonFile(scale int) []string {
	var lines []string
	add := func(text string) {
		v, err := vrpc.ParseJson([]byte(text))
		result := "error"
		if err == nil {
			result = packed(v)
		}
		lines = append(lines, b64(text)+"\t"+result)
	}
	for _, text := range []string{
		// the literals, and what is not one
		`null`, `true`, `false`, ` null `, "\tnull\r\n", `nul`, `nulll`, `True`, `tru`, `falsey`, ``, ` `,
		// numbers
		`0`, `-0`, `1`, `-1`, `42`, `9223372036854775807`, `-9223372036854775808`, `9223372036854775808`,
		`-9223372036854775809`, `123456789012345678901234567890`, `0.5`, `-0.0`, `1.5`, `1e3`, `1E3`, `1e+3`, `1e-3`,
		`1.0`, `100.25e-2`, `1e308`, `1e309`, `-1e309`, `1e-400`, `0.1e1`, `01`, `-01`, `+1`, `1.`, `.5`, `-`, `1e`, `1e+`,
		`0x10`, `1_000`, `NaN`, `Infinity`, `--1`, `1 2`, `1x`, `12345678901234567890.5`,
		// strings
		`""`, `"text"`, `"a\"b\\c\/d"`, `"\b\f\n\r\t"`,
		// escapes of UTF-16 code units: a letter, an accent, CJK, a control
		// character, a surrogate pair, and surrogates that are not a pair
		q(u(0x41), u(0xe9), u(0x65e5)), q(u(0)), q(u(0xd83d), u(0xde00)), q(upper(0xd83d), upper(0xde00)),
		q(u(0xd83d)), q(u(0xde00)), q(u(0xd83d), "x"), q(u(0xd83d), u(0x41)), q(u(0xde00), u(0xd83d)),
		q(u(0xd83d), u(0xd83d), u(0xde00)), q(u(0xfffd)), q(u(0x200b)), q("a", u(0xd83d)),
		`"\x41"`, `"\a"`, `"\u12"`, `"\u12G4"`, `"\U00000041"`, `"unterminated`, `"`, "\"tab\there\"", "\"line\nbreak\"",
		"\"nul\x00inside\"", "\"del\x7finside\"", `'single'`, `"emoji 😀 and é"`,
		"\"bad \xff byte\"", "\"cut \xe2\x82\"", "\"cut \xe2\x82 more\"", "\"surrogate \xed\xa0\x80\"",
		"\"overlong \xc0\xaf\"", "\"beyond \xf4\x90\x80\x80\"", "\"\xf0\x9f\x98\x80\"",
		// strings that are binary, extensions, or look like them
		`"base64,AQI"`, `"base64,"`, `"base64,AQI="`, `"base64,!!!"`, `"base64,AQ"`, `"base64,A"`, `"Base64,AQI"`,
		`" base64,AQI"`, `"base64,AAEC/+8"`, `"base64,AA\nEC"`,
		`"data:application/x-msgpack-ext;base64,BwEC"`, `"data:application/x-msgpack-ext;base64,Bw"`,
		`"data:application/x-msgpack-ext;base64,"`, `"data:application/x-msgpack-ext;base64,!"`,
		`"data:application/x-msgpack-ext;AQI"`, `"data:application/x-msgpack-ext;"`,
		// The type of a big integer, with a canonical payload: Go keeps an
		// extension opaque whatever its type, and its bytes are the big integer's.
		`"data:application/x-msgpack-ext;base64,AXs"`,
		`"0x7b"`, `"0x01e2afx-03"`, `"2026-10-30T07:28:39.075Z"`,
		// lists and maps
		`[]`, `[ ]`, `[1]`, `[1,2,3]`, `[ 1 , "a" , null , true , 1.5 ]`, `[[],[[]],{}]`, `[1,]`, `[,1]`, `[1 2]`, `[`, `]`,
		`[1`, `[1,`, `{}`, `{ }`, `{"a":1}`, `{ "b" : 2 , "a" : 1 }`, `{"a":{"b":{"c":[1,{"d":null}]}}}`,
		`{"a":1,"a":2}`, `{"a":1,"b":2,"a":3}`, `{"":0}`, `{"k\u0000":"v"}`, `{"base64,AQI":"base64,AQI"}`,
		`{"a":1,}`, `{,}`, `{"a"}`, `{"a":}`, `{a:1}`, `{1:1}`, `{"a" 1}`, `{"a":1 "b":2}`, `{`, `}`, `{"a":1`,
		// what follows the value
		`{}{}`, `[1,2] 3`, `{} x`, `null null`, `{}}`, `{}} garbage`, `[]]`, `1]`, `"a"}`, `true ]`, `{} `,
		// an envelope
		`{"args":["world"],"fn":"greet","rid":1,"sla":5000,"t":2}`,
		`{"cid":712398211,"m":"vRPC","rid":0,"t":0,"tok":"` + anchor + `","v":1}`,
		`{"code":5,"err":"function not found","rid":3,"t":7}`,
	} {
		add(text)
	}
	// depth: a value may be 1000 levels down, and no deeper
	for _, depth := range []int{999, 1000, 1001, 1002, 1100} {
		add(strings.Repeat("[", depth) + strings.Repeat("]", depth))
		add(strings.Repeat(`{"k":`, depth) + "1" + strings.Repeat("}", depth))
	}
	// what Jsonify writes for random values reads back
	for i := 0; i < 300*scale; i++ {
		add(val.Jsonify(randomValue(0)))
	}
	return lines
}

var alphabet = append([]rune("abcXYZ019 _-\"\\/\b\f\n\r\t\x00\x1f\x7f,:{}[]"),
	0xe9, 0x65e5, 0x672c, 0x1f600, 0x200b, 0xfffd) // an accent, CJK, beyond the basic plane, not printable, the replacement character

func randomText() string {
	n := rng.Intn(12)
	r := make([]rune, n)
	for i := range r {
		r[i] = alphabet[rng.Intn(len(alphabet))]
	}
	s := string(r)
	if rng.Intn(12) == 0 {
		s += "\xff" // a byte that is not UTF-8
	}
	return s
}

func randomValue(depth int) val.Value {
	limit := 12
	if depth >= 4 {
		limit = 9
	}
	switch rng.Intn(limit) {
	case 0:
		return val.Null
	case 1:
		return val.Boolean(rng.Intn(2) == 0)
	case 2:
		return val.Long(rng.Int63n(2000) - 1000)
	case 3:
		return val.Long(int64(rng.Uint64()))
	case 4:
		return val.Double([]float64{0, -0.0, 1.5, -12.34, 1e21, 1e-7, 123456.789, math.MaxFloat64, math.SmallestNonzeroFloat64,
			math.Inf(1), math.Inf(-1), math.NaN(), rng.NormFloat64() * 1e6}[rng.Intn(13)])
	case 5:
		return val.Utf8(randomText())
	case 6:
		b := make([]byte, rng.Intn(9))
		rng.Read(b)
		return val.Raw(b, true)
	case 7:
		return val.BigInt(new(big.Int).Lsh(big.NewInt(rng.Int63()-rng.Int63()), uint(rng.Intn(80))))
	case 8:
		return val.UnixTime(rng.Int63n(4e9), int64(rng.Intn(2))*int64(rng.Intn(int(time.Second))))
	case 9:
		n := rng.Intn(5)
		items := make([]val.Value, n)
		for i := range items {
			items[i] = randomValue(depth + 1)
		}
		return val.ImmutableList(items)
	case 10:
		n := rng.Intn(5)
		m := make(map[string]val.Value, n)
		for i := 0; i < n; i++ {
			m[randomText()] = randomValue(depth + 1)
		}
		return val.ImmutableMapOf(m)
	default:
		data := make([]byte, 1+rng.Intn(6))
		rng.Read(data)
		data[0] = byte(3 + rng.Intn(100)) // an extension the format does not define
		return val.Unknown(data)
	}
}
