class Model(object):

    @staticmethod
    def _ensure_path_ends_with_slash(path):
        if not path.endswith('/'):
            return path + '/'

        return path
