import matplotlib.pyplot as plt
import pandas as pd
import seaborn

def album_counts_eda(binwidth = None, cutoff = None):
    """
    Input:
        binwidth -- integer for the width of histogram bins
        divide -- integer to decide what counts are high vs. low
    Output:
        None
    """

    # Lower
    album_counts_df = pd.read_csv('data/album_counts.csv')
    counts = album_counts_df['count']
    counts_low = counts[counts < cutoff]

    plt.hist(counts,
             log = True,
             normed = False,
             bins = range(min(counts_low), max(counts_low) + binwidth, binwidth))
    plt.ylabel('Log Album Counts')
    plt.xlabel('Albums')
    plt.show()
    plt.savefig('images/album_counts_lower.png')

    # Upper
    counts_high = counts[counts >= cutoff]

    plt.hist(counts,
             log = False,
             normed = False,
             bins = range(min(counts_high), max(counts_high) + binwidth, binwidth))
    plt.ylabel('Album Counts')
    plt.xlabel('Albums')
    plt.show()
    plt.savefig('images/album_counts_higher.png')

def tag_counts_eda():
    """
    Prints the most common tags in descending order
    """
    sf = graphlab.SFrame.read_csv('data/album_url_to_album_tag.csv')
    print sf.to_dataframe()['album_tag'].value_counts()

if __name__ == "__main__":
    album_counts_eda(binwidth = 10,
                     cutoff = 1500)
