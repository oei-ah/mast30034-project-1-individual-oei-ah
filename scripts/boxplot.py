import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from urllib.request import urlretrieve
import os
import shapefile as shp

# PUT THIS IN SCRIPTS

def plot_boxplot(df, features, title='Box Plot of Features', xlabel='Features', figsize=(10, 6)):

    # Create a DataFrame for the features to plot
    plot_data = df[features]

    # Melt the DataFrame to long format for seaborn
    plot_data_melted = plot_data.melt(var_name='Feature', value_name='Value')

    # Initialize the matplotlib figure
    plt.figure(figsize=figsize)

    # Create the box plot using seaborn
    sns.boxplot(x='Feature', y='Value', data=plot_data_melted)

    # Customize the plot
    plt.title(title, fontsize=15)
    plt.xlabel(xlabel, fontsize=15)
    plt.xticks(fontsize=15)

    # Show the plot
    plt.show()
    
    
