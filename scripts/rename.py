import os
import sys

def rename_in_file(path, foo, bar):
    with open(path, 'r', errors='replace') as f:
        content = f.read()
    if foo in content:
        with open(path, 'w') as f:
            f.write(content.replace(foo, bar))

def rename_all(root, foo, bar):
    all_dirs = []
    all_files = []

    for dirpath, dirnames, filenames in os.walk(root, topdown=True):
        dirnames[:] = [d for d in dirnames if not d.startswith('.')]
        all_dirs.append(dirpath)
        for filename in filenames:
            if not filename.startswith('.'):
                all_files.append(os.path.join(dirpath, filename))

    for path in all_files:
        rename_in_file(path, foo, bar)
        new_name = os.path.basename(path).replace(foo, bar)
        if new_name != os.path.basename(path):
            os.rename(path, os.path.join(os.path.dirname(path), new_name))

    for dirpath in reversed(all_dirs):
        new_dir = os.path.basename(dirpath).replace(foo, bar)
        if new_dir != os.path.basename(dirpath):
            new_path = os.path.join(os.path.dirname(dirpath), new_dir)
            os.rename(dirpath, new_path)

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("usage: rename.py <root> <foo> <bar>")
        sys.exit(1)
    rename_all(sys.argv[1], sys.argv[2], sys.argv[3])
