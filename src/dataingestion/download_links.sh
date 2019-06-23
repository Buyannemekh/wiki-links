# All links
wget "https://dumps.wikimedia.org/enwiki/latest/" -O all_links.txt

# Get all meta history pages  with bz2 extention for future
grep -Eoi '<a [^>]+>' all_links.txt | grep -Eo 'href="[^\"]+"'| grep -Eo 'enwiki-latest-pages-meta-history[^/"]*.bz2' > history_bz2_links.txt


# Get unique urls from the bz2 urls
sort history_bz2_links.txt | sort -u > bz2_links.txt


# Append https for each line
while read line; do echo "https://dumps.wikimedia.org/enwiki/latest/$line"; done < bz2_links.txt > http_bz2_links.txt


# single file download
# curl https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-pages-meta-history10.xml-p3040817p3046511.bz2  | aws s3 cp - s3://wiki-meta/tt10_t

# all file download

