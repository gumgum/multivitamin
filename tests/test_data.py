from multivitamin.data.response.data import MediaAnn, Point

p = Point(1, 2)
print(p)
print(type(p))
print(type(Point))
assert isinstance(p, type(Point()))
# import json
# m = MediaAnn(w=44)
# print(type(m))
# print(m.__class__)
# assert(isinstance(m, MediaAnn))
# print(m['w'])
# print(json.dumps(m.to_dict(), indent=2))
# print(m.to_json(2))
# x = m.to_dict()

# x['z'] = 2.0
# print(x)
# x['w'] = 33
# re = MediaAnn().from_dict(x)
# print(re)
