# All links
wget "https://dumps.wikimedia.org/enwiki/latest/" -O all_links.txt

# Get all meta history pages  with bz2 extenstion for future
grep -Eoi '<a [^>]+>' all_links.txt | grep -Eo 'href="[^\"]+"'| grep -Eo 'enwiki-latest-pages-meta-history[^/"]*.bz2' > cleaned_bz2_links.txt

# Get unique urls from the bz2 urls
sort cleaned_bz2_links.txt | sort -u > bz2_links.txt

# Append https for each line
while read line; do echo "https://dumps.wikimedia.org/enwiki/latest/$line"; done < bz2_links.txt > bz2_http_links.txt


