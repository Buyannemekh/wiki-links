while read p; do
    export NAME=${p/https:\/\/dumps.wikimedia.org\/enwiki\/latest\/enwiki-latest-pages-meta-/}
    # echo $NAME
    curl "$p" | aws s3 cp - s3://wiki-current-part3/"$NAME";
done < http_bz2_current_links_part3.txt
