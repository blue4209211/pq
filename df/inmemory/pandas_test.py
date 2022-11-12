import pandas as pd

s1 = pd.Series(range(0, 10000000))
s2 = s1[s1/2 == 0]
s3 = s1+10
print(len(s2))
print(len(s3))