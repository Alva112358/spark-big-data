import numpy as np
import pandas as pd
from wordcloud import WordCloud, ImageColorGenerator
import matplotlib.pyplot as plt
from PIL import Image


def draw_cloud(read_name):
    image = Image.open('map.jpg')
    graph = np.array(image) 
    wc = WordCloud(width = 1000, height = 700, font_path='Arial.ttf', background_color='white', max_words=100)
    fp = pd.read_csv(read_name, encoding='utf-8')
    name = list(fp.name)  
    value = fp.value
    for i in range(len(name)):
        name[i] = str(name[i])
    dic = dict(zip(name, value))  
    wc.generate_from_frequencies(dic)  
    image_color = ImageColorGenerator(graph)
    plt.imshow(wc)
    plt.axis("off")  
    wc.to_file('wordcloudneg.png')  


if __name__ == '__main__':
    file_name = input()
    draw_cloud(file_name)
