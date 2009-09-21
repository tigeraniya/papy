svn propset -R svn:mime-type text/css        `find . -name .svn -type f -prune -o -name "*.css"`
svn propset -R svn:mime-type text/javascript `find . -name .svn -type f -prune -o -name "*.js"`
svn propset -R svn:mime-type text/html       `find . -name .svn -type f -prune -o -name "*.html"`
svn propset -R svn:mime-type image/x-png     `find . -name .svn -type f -prune -o -name "*.png"`
