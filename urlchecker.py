import sys
import urllib2
import pandas as pd
def get_url_nofollow(url):
    try:
        response = urllib2.urlopen(url)
        code = response.getcode()
        return code
    except urllib2.HTTPError as e:
        return e.code
    except:
        return 0


if __name__ == "__main__":
    df = pd.DataFrame.from_csv('apps_data.csv')
    print df
    for index,row in df.iterrows():
        row['status'] = res = get_url_nofollow(index)
        print res
    df.to_csv('output.csv')