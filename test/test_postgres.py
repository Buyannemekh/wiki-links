import re


# return list of link titles from a text if exist, else return empty list
def find_links(text):
    try:
        match_list = re.findall('\[\[[^\[\]]+\]\]', text[0])
        return match_list
    except:
        return []
