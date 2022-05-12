import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import palettable

heat = pd.read_csv("/Users/bc/Desktop/heat.csv")
plt.figure(figsize=(18,8))
heat = heat.pivot("category", "lable", "len")

sns.heatmap(heat,cmap=palettable.cartocolors.diverging.ArmyRose_7.mpl_colors, fmt='g',annot=True, linewidths=5)
plt.savefig('heatmap.png')
