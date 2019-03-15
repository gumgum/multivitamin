from multivitamin.data.response.data import MediaAnn

import json
m = MediaAnn()
print(json.dumps(m.to_dict(), indent=2))

x = m.to_dict()

x['z'] = 2.0
print(x)
x['w'] = 33
re = MediaAnn().from_dict(x)
print(re)

