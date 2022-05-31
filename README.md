# Dedupe-files

Multithreaded, non-recursive file deduplicator

```
dedupe_files --cores 4
```

```
$ cat test.sh
echo a > first_a
echo a > second_a
echo a > third_a
echo b > first_b
echo c > first_c
echo c > second_c
echo c > third_c

# Create a directory of duplicate files
$ ./test.sh

$ ls
first_a  first_b  first_c  second_a  second_c  test.sh  third_a  third_c

# Dedupe using 8 cores
$ dedupe_files --cores 8
Entries: 8

# Check that only unique files are left
$ ls
first_b  second_a  test.sh  third_c
```
