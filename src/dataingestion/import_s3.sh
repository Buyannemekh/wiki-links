while read p; do  curl "$p" | aws s3 cp - s3://wikipedia-article-sample-data/"$p"; done < text_http.txt
