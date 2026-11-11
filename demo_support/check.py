import json,hashlib,urllib.request
from source import start,ROOT
server,base=start()
try:
    import struct,zlib
    data=urllib.request.urlopen(base+'/frames').read();size=struct.unpack('>H',data[2:4])[0]
    assert size==26 and zlib.crc32(data[:4+size])&0xffffffff==struct.unpack('>I',data[4+size:8+size])[0]
    payload=struct.unpack('<IHHHqd',data[4:4+size]);assert payload[:4]==(7,65535,1,1)
    assert len(urllib.request.urlopen(base+'/frames?cut=1').read())==len(data)-10
    assert json.load(urllib.request.urlopen(base+'/channels/1'))['version']==1
finally:
    server.shutdown();server.server_close()
print('本地输入协议与故障样本检查通过')
