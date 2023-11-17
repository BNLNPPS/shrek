function check_input_files() {
# Given an array of filenames, follow the link and require that the link
# actually leads to a file.  Emit an error if it does not.
   IN=("$@")
   for f in "${IN[@]}";
      do
          if [ ! -e $f ] ; then
             echo
             echo "WARN ============================================================"
             echo "WARN Input file $f does not seem to exist..."
             echo "WARN ... should be at " `readlink -f $f`
             echo "WARN ============================================================"
             echo
          fi          
      done
}
