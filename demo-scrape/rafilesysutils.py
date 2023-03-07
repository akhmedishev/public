import os
from urllib.parse import urlsplit
from urllib.request import url2pathname

class FoldersProvider:
    def __init__(self, base_directory, postfix):
        self.folders_dict = dict()
        self.postfix = postfix
        self.base_directory = base_directory

    def nid_folder(self, sub_name):
        if sub_name not in self.folders_dict:
            new_sub = os.path.join(self.base_directory, sub_name+self.postfix)
            i = 0
            while os.path.exists(new_sub):
                i += 1
                new_sub = os.path.join(self.base_directory, sub_name+self.postfix+'-'+str(i))
            self.folders_dict[sub_name] = new_sub
        return self.folders_dict[sub_name]



def save_to_file(folders_provider, url, body):
    parsed = urlsplit(url)
    root_dir = folders_provider.nid_folder(url2pathname(parsed.netloc))
    part_dir, filename = os.path.split(url2pathname(parsed.path))
    if not filename:
        part_dir, filename = os.path.split(part_dir)
    dir = os.path.join(root_dir, part_dir.lstrip(os.path.sep))
    #print(f"root_dir: {root_dir}\npart_dir: {part_dir}\ndir: {dir}")
    os.makedirs(dir, exist_ok=True)
    if parsed.query:
        filename += '_' + url2pathname(parsed.query.replace('&', '_'))
    if parsed.fragment:
        filename += '_' + url2pathname(parsed.fragment)
    if not filename:
        filename = '_'
    i = 0
    full_filepath = os.path.join(dir, filename+'.html')
    while os.path.exists(full_filepath):
        i += 1
        full_filepath = os.path.join(dir, filename + '-' + str(i)+'.html')
    if isinstance(body, str):
        with open(full_filepath, 'w', encoding='utf-8') as fout:
            fout.write(body)
    else:
        with open(full_filepath, 'wb') as fout:
            fout.write(body)
    #print('debug:', full_filepath)
    return os.path.relpath(full_filepath, (root_dir))  # os.path.dirname(root_dir)


