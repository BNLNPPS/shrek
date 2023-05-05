# Simplistic function to get the list of files (pfns) contained in a dataset.
# ... does not required that the list of files is unique
function get_file_list() {
    rucio list-file-replicas --pfns $1 | sed 's/file:\/\/localhost//g'
}
