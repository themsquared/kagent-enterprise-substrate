#!/usr/bin/env python3
"""Regenerate lib/schema.json from a kagent-enterprise release.

kagent-enterprise serves plain gRPC / gRPC-Web with no reflection and no JSON
transcoding, so Scope carries a compact message schema. It comes from two
places a release already ships:

  * the controller binary, which embeds every compiled FileDescriptorProto
    (kagent.api.v1alpha1.*, ateapi.*), and
  * the UI bundle, which embeds the A2A 1.0 descriptor (lf.a2a.v1) as base64.

Usage (needs `pip install protobuf`, docker, and a reachable UI):
  python3 tools/gen-schema.py <controller-image> <ui-js-bundle> > lib/schema.json
"""
import base64, json, re, subprocess, sys, tempfile, os
from google.protobuf import descriptor_pb2 as d

KEEP = ('kagent.api.v1alpha1', 'ateapi', 'lf.a2a.v1')


def varint(data, i):
    r = sh = 0
    while True:
        b = data[i]; i += 1; r |= (b & 0x7f) << sh; sh += 7
        if b < 0x80: return r, i


def from_binary(data):
    """Scan a Go binary for embedded FileDescriptorProto blobs."""
    pat = rb'\x0a([\x01-\x7f])((?:kagent/[a-z0-9_/]+|ateapi)\.proto)'
    found = {}
    for m in re.finditer(pat, data):
        if len(m.group(2)) != m.group(1)[0]: continue
        name, s = m.group(2).decode(), m.start()
        bounds, i = [], s
        while True:                      # walk top-level fields to find candidate ends
            try: key, j = varint(data, i)
            except IndexError: break
            f, wt = key >> 3, key & 7
            if f < 1 or f > 50 or wt not in (0, 2): break
            if wt == 0: _, j = varint(data, j)
            else: l, j = varint(data, j); j += l
            if j > len(data): break
            i = j; bounds.append(i)
        for e in reversed(bounds):       # longest blob that parses as this file
            fd = d.FileDescriptorProto()
            try: fd.ParseFromString(data[s:e])
            except Exception: continue
            if fd.name == name and (fd.message_type or fd.service):
                if name not in found or fd.ByteSize() > found[name].ByteSize(): found[name] = fd
                break
    return list(found.values())


def from_ui(js):
    out = []
    for b in re.findall(r'=sw\(`([A-Za-z0-9+/_-]{40,})`', js):
        b = b.replace('-', '+').replace('_', '/'); b += '=' * (-len(b) % 4)
        fd = d.FileDescriptorProto()
        try: fd.ParseFromString(base64.b64decode(b))
        except Exception: continue
        if fd.package == 'lf.a2a.v1': out.append(fd)
    return out


SCALAR = {v: k[5:].lower() for k, v in d.FieldDescriptorProto.Type.items()}


def compile_files(files):
    msgs, enums, services = {}, {}, {}

    def walk(prefix, m):
        full = f'{prefix}.{m.name}'
        for n in m.nested_type: walk(full, n)
        for e in m.enum_type: enums[f'{full}.{e.name}'] = {v.number: v.name for v in e.value}
        fields = {}
        for f in m.field:
            fields[f.number] = [f.name, SCALAR[f.type], f.label == 3, f.type_name.lstrip('.') or None]
        msgs[full] = {'f': fields, **({'map': 1} if m.options.map_entry else {})}

    for fd in files:
        if not fd.package.startswith(KEEP): continue
        for m in fd.message_type: walk(fd.package, m)
        for e in fd.enum_type: enums[f'{fd.package}.{e.name}'] = {v.number: v.name for v in e.value}
        for s in fd.service:
            for mm in s.method:
                services[f'{fd.package}.{s.name}/{mm.name}'] = [mm.input_type.lstrip('.'), mm.output_type.lstrip('.')]
    return {'messages': msgs, 'enums': enums, 'methods': services}


if __name__ == '__main__':
    image, ui_js = sys.argv[1], sys.argv[2]
    with tempfile.TemporaryDirectory() as tmp:
        cid = subprocess.check_output(['docker', 'create', image], text=True).strip()
        try: subprocess.check_call(['docker', 'cp', f'{cid}:/manager', os.path.join(tmp, 'manager')])
        finally: subprocess.call(['docker', 'rm', cid], stdout=subprocess.DEVNULL)
        files = from_binary(open(os.path.join(tmp, 'manager'), 'rb').read())
    files += from_ui(open(ui_js).read())
    json.dump(compile_files(files), sys.stdout, separators=(',', ':'))
