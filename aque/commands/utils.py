import subprocess


_stty_size = []
def stty_size():
    if not _stty_size:
        proc = subprocess.Popen(['stty', 'size'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        out, err = proc.communicate()
        if out:
            _stty_size.extend(map(int, out.strip().split()))
        else:
            _stty_size.extend((None, None))
    return _stty_size[0], _stty_size[1]


def trim_to_width(input_, width):
    if width is None:
        return input_
    if len(input_) > width - 3:
        return input_[:width-3] + '...'
