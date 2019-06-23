while read p; do
    export NAME=${p/https:\/\/dumps.wikimedia.org\/enwiki\/latest\/enwiki-latest-pages-meta-/}
    #echo $NAME
    curl "$p" | aws s3 cp - s3://wikipedia-article-sample-data/"$NAME";
done < text_http.txt
