#
# Copyright (c) 2026 Couchbase, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
"""List BC string constants that look like 'org.bouncycastle.*' but are not class or
package names, i.e. system/security property names that shade relocation must not rewrite.
Reads whole CONSTANT_Utf8 entries from each class's constant pool (no regex truncation).
Usage: python3 dev/bc_propnames.py <jar>... (the original, unshaded BC jars)."""
import re, struct, sys, zipfile

def utf8_constants(data):
    if data[:4] != b'\xca\xfe\xba\xbe':
        return
    count = struct.unpack('>H', data[8:10])[0]
    i, idx = 10, 1
    sizes = {3: 4, 4: 4, 5: 8, 6: 8, 7: 2, 8: 2, 9: 4, 10: 4, 11: 4, 12: 4, 15: 3, 16: 2, 17: 4, 18: 4, 19: 2, 20: 2}
    while idx < count:
        tag = data[i]
        if tag == 1:
            n = struct.unpack('>H', data[i + 1:i + 3])[0]
            yield data[i + 3:i + 3 + n].decode('utf-8', 'replace')
            i += 3 + n
        else:
            i += 1 + sizes[tag]
            if tag in (5, 6):
                idx += 1
        idx += 1

classes, strings, resources = set(), set(), set()
for jar in sys.argv[1:]:
    z = zipfile.ZipFile(jar)
    resources.update(n.rsplit('.', 1)[0].replace('/', '.') for n in z.namelist() if not n.endswith('.class'))
    for n in z.namelist():
        if n.endswith('.class') and not n.startswith('META-INF/'):
            classes.add(n[:-6].replace('/', '.'))
            strings.update(s for s in utf8_constants(z.read(n)) if s.startswith('org.bouncycastle.'))
packages = {c.rsplit('.', 1)[0] for c in classes}
packages |= {p.rsplit('.', i)[0] for p in list(packages) for i in range(1, 8) if p.count('.') >= i}
for s in sorted(strings):
    # Property names are plain dotted identifiers (may contain '-'); class names given as
    # strings (with '$'), resource bundles, messages and '|' lists are relocated as usual.
    if not re.fullmatch(r'org\.bouncycastle\.[A-Za-z0-9_.\-]+', s) or s.endswith('.'):
        continue
    if s in classes or s in packages or s in resources:
        continue
    # A CamelCase last segment with no '_'/'-' is a class name for an algorithm not in this jar
    # (e.g. ...sm9.SM9KEMSpi), not a property.
    if re.fullmatch(r'[A-Z][A-Za-z0-9]*', s.rsplit('.', 1)[1]):
        continue
    print(s)
