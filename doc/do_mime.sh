svn propset -R svn:mime-type text/css `find .build/html/ -name .svn \
  -type f -prune -o -name *.css`
svn propset -R svn:mime-type text/javascript `find .build/html/ \
    -name .svn -type f -prune -o -name *.js`
svn propset -R svn:mime-type image/x-png `find .build/html/ \
      -name .svn -type f -prune -o -name *.png`
svn propset -R svn:mime-type text/html `find .build/html/ -name .svn \
        -type f -prune -o -name *.html`
