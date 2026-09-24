// Zero-dependency gRPC-Web client for kagent-enterprise.
//
// kagent-enterprise 1.0 serves its API as plain gRPC / gRPC-Web: no
// reflection, no JSON transcoding. Scope carries a compact schema generated
// from the release itself (tools/gen-schema.py -> lib/schema.json) and this
// file encodes and decodes against it. Messages are plain JS objects keyed by
// the proto field names (snake_case); enums decode to their value names.
import { readFileSync } from 'node:fs';

const SCHEMA = JSON.parse(readFileSync(new URL('./schema.json', import.meta.url)));

// Well-known types the release protos import but do not define.
Object.assign(SCHEMA.messages, {
  'google.protobuf.Timestamp': { f: { 1: ['seconds', 'int64'], 2: ['nanos', 'int32'] } },
  'google.protobuf.Duration':  { f: { 1: ['seconds', 'int64'], 2: ['nanos', 'int32'] } },
  'google.protobuf.Empty':     { f: {} },
  'google.protobuf.Struct':    { f: { 1: ['fields', 'message', true, 'google.protobuf.Struct.FieldsEntry'] } },
  'google.protobuf.Struct.FieldsEntry': { map: 1, f: { 1: ['key', 'string'], 2: ['value', 'message', false, 'google.protobuf.Value'] } },
  'google.protobuf.Value': { f: {
    1: ['null_value', 'enum'], 2: ['number_value', 'double'], 3: ['string_value', 'string'],
    4: ['bool_value', 'bool'], 5: ['struct_value', 'message', false, 'google.protobuf.Struct'],
    6: ['list_value', 'message', false, 'google.protobuf.ListValue'] } },
  'google.protobuf.ListValue': { f: { 1: ['values', 'message', true, 'google.protobuf.Value'] } },
});
// Index fields by name for encoding.
for (const m of Object.values(SCHEMA.messages))
  m.byName = Object.fromEntries(Object.entries(m.f).map(([n, f]) => [f[0], [Number(n), ...f.slice(1)]]));

// ── wire primitives ──────────────────────────────────────────────────────────
function readVarint(buf, pos) {
  let result = 0n, shift = 0n;
  for (;;) {
    const b = buf[pos++];
    result |= BigInt(b & 0x7f) << shift;
    if (b < 0x80) return [result, pos];
    shift += 7n;
  }
}
function writeVarint(out, v) {
  v = BigInt.asUintN(64, BigInt(v));
  while (v > 0x7fn) { out.push(Number(v & 0x7fn) | 0x80); v >>= 7n; }
  out.push(Number(v));
}
const VARINT = new Set(['int32', 'int64', 'uint32', 'uint64', 'sint32', 'sint64', 'bool', 'enum']);

// ── decode ───────────────────────────────────────────────────────────────────
function scalar(type, raw) {
  switch (type) {
    case 'bool': return raw !== 0n;
    case 'int32': return Number(BigInt.asIntN(32, raw));
    case 'int64': return Number(BigInt.asIntN(64, raw));
    case 'sint32': case 'sint64': return Number((raw >> 1n) ^ -(raw & 1n));
    default: return Number(raw);
  }
}
export function decode(typeName, buf, start = 0, end = buf.length) {
  const m = SCHEMA.messages[typeName];
  const out = {};
  let pos = start;
  while (pos < end) {
    let key; [key, pos] = readVarint(buf, pos);
    const num = Number(key >> 3n), wt = Number(key & 7n);
    const f = m?.f[num];
    let val;
    if (wt === 0) { let r; [r, pos] = readVarint(buf, pos); val = r; }
    else if (wt === 1) { val = buf.readDoubleLE(pos); pos += 8; }
    else if (wt === 5) { val = buf.readFloatLE(pos); pos += 4; }
    else if (wt === 2) {
      let len; [len, pos] = readVarint(buf, pos); len = Number(len);
      const s = pos, e = pos + len; pos = e;
      if (!f) continue;
      const [, type, , ref] = f;
      if (type === 'string') val = buf.toString('utf8', s, e);
      else if (type === 'bytes') val = buf.subarray(s, e);
      else if (type === 'message') val = decode(ref, buf, s, e);
      else {                                     // packed repeated scalars
        const arr = out[f[0]] ?? (out[f[0]] = []);
        for (let p = s; p < e;) { let r; [r, p] = readVarint(buf, p); arr.push(fmt(f, r)); }
        continue;
      }
    } else throw new Error(`unsupported wire type ${wt} in ${typeName}`);
    if (!f) continue;
    if (wt === 0) val = fmt(f, val);
    const [name, type, repeated, ref] = f;
    if (type === 'message' && SCHEMA.messages[ref]?.map) {
      (out[name] ??= {})[val.key ?? ''] = val.value;
    } else if (repeated) (out[name] ??= []).push(val);
    else out[name] = val;
  }
  return typeName.startsWith('google.protobuf.') ? unwrapWkt(typeName, out) : out;
}
function fmt([, type, , ref], raw) {
  if (type === 'enum') return SCHEMA.enums[ref]?.[Number(raw)] ?? Number(raw);
  return scalar(type, raw);
}
function unwrapWkt(t, o) {
  if (t === 'google.protobuf.Timestamp') return new Date((o.seconds ?? 0) * 1000 + (o.nanos ?? 0) / 1e6).toISOString();
  if (t === 'google.protobuf.Struct') return o.fields ?? {};
  if (t === 'google.protobuf.ListValue') return o.values ?? [];
  if (t === 'google.protobuf.Value') {
    if ('struct_value' in o) return o.struct_value;
    if ('list_value' in o) return o.list_value;
    return o.string_value ?? o.number_value ?? o.bool_value ?? null;
  }
  return o;
}

// ── encode (requests are small: strings, refs, pages, messages) ──────────────
export function encode(typeName, obj, out = []) {
  const m = SCHEMA.messages[typeName];
  if (!m) throw new Error(`unknown message ${typeName}`);
  for (const [name, v] of Object.entries(obj ?? {})) {
    if (v === undefined || v === null) continue;
    const f = m.byName[name];
    if (!f) throw new Error(`${typeName} has no field ${name}`);
    const [num, type, repeated, ref] = f;
    for (const item of repeated ? v : [v]) {
      if (type === 'message') {
        const sub = encode(ref, item, []);
        writeVarint(out, (num << 3) | 2); writeVarint(out, sub.length); out.push(...sub);
      } else if (type === 'string' || type === 'bytes') {
        const b = Buffer.from(item);
        writeVarint(out, (num << 3) | 2); writeVarint(out, b.length); out.push(...b);
      } else if (VARINT.has(type)) {
        let n = item;
        if (type === 'enum' && typeof item === 'string')
          n = Number(Object.entries(SCHEMA.enums[ref]).find(([, s]) => s === item)?.[0] ?? 0);
        if (type === 'bool') n = item ? 1 : 0;
        writeVarint(out, num << 3); writeVarint(out, n);
      } else throw new Error(`encode: unsupported type ${type}`);
    }
  }
  return out;
}

// ── transport ────────────────────────────────────────────────────────────────
const frame = bytes => {
  const b = Buffer.alloc(5 + bytes.length);
  b.writeUInt32BE(bytes.length, 1); Buffer.from(bytes).copy(b, 5);
  return b;
};
const parseTrailers = s => Object.fromEntries(s.split('\r\n').filter(Boolean)
  .map(l => { const i = l.indexOf(':'); return [l.slice(0, i).trim().toLowerCase(), l.slice(i + 1).trim()]; }));

export class GrpcError extends Error {
  constructor(code, message) { super(message || `grpc status ${code}`); this.code = code; }
}

// Client for one base URL, e.g. http://127.0.0.1:8001/api/proxy/cluster/<cluster>/api
// getHeaders() is async so the caller can refresh a bearer token.
export function client(base, getHeaders = async () => ({})) {
  async function* call(method, req, { headers = {}, signal, timeout = 20_000 } = {}) {
    const types = SCHEMA.methods[method];
    if (!types) throw new Error(`unknown method ${method}`);
    const res = await fetch(`${base}/${method}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/grpc-web+proto', 'X-Grpc-Web': '1',
                 ...(await getHeaders()), ...headers },
      body: frame(encode(types[0], req)),
      signal: signal ?? AbortSignal.timeout(timeout),
    });
    const hs = res.headers.get('grpc-status');
    if (!res.ok) throw new GrpcError(-res.status, `HTTP ${res.status}: ${(await res.text()).slice(0, 200)}`);
    if (hs && hs !== '0') throw new GrpcError(Number(hs), decodeURIComponent(res.headers.get('grpc-message') ?? ''));
    let pending = Buffer.alloc(0);
    for await (const chunk of res.body) {
      pending = Buffer.concat([pending, chunk]);
      while (pending.length >= 5) {
        const len = pending.readUInt32BE(1);
        if (pending.length < 5 + len) break;
        const flag = pending[0], body = pending.subarray(5, 5 + len);
        pending = pending.subarray(5 + len);
        if (flag & 0x80) {
          const t = parseTrailers(body.toString());
          if (t['grpc-status'] && t['grpc-status'] !== '0')
            throw new GrpcError(Number(t['grpc-status']), decodeURIComponent(t['grpc-message'] ?? ''));
          return;
        }
        yield decode(types[1], body);
      }
    }
  }
  return {
    unary: async (method, req, opts) => { for await (const m of call(method, req, opts)) return m; return {}; },
    stream: call,
  };
}
