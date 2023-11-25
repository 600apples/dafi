import dill
import pickle

args = [1,2, 3, 4, 5, 6, 7, 8, 9, 10]
kwargs = {"a":1, "b":2, "c":3, "d":4, "e":5}
intotal = (args, kwargs)


p1 = dill.dumps(intotal, protocol=5)

p2 = pickle.dumps(intotal, protocol=5)

print(p1)
print(p2)