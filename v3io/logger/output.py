import sys
import logging
import logging.handlers


class Output(object):

    def __init__(self, severity):
        self._severity = severity

    def _add_handler(self, logger, handler):

        handler.setLevel(self._severity.upper())

        logger.addHandler(handler)


class Stdout(Output):

    def __init__(self, severity, colors):
        self._colors = colors
        super(Stdout, self).__init__(severity)

    def create_handler(self, logger_instance):
        import v3io.logger.formatter

        # tty friendliness:
        # on - disable colors if stdout is not a tty
        # always - never disable colors
        # off - always disable colors
        if self._colors == 'off':
            enable_colors = False
        elif self._colors == 'always':
            enable_colors = True
        else:  # on - colors when stdout is a tty
            enable_colors = sys.stdout.isatty()

        handler = logging.StreamHandler(sys.__stdout__)
        handler.setFormatter(v3io.logger.formatter.HumanReadable(enable_colors))

        self._add_handler(logger_instance, handler)


class File(Output):

    def __init__(self, severity, dir, file_name, max_num_files):
        if dir is None:
            raise RuntimeError('File output requires directory to be defined')

        if file_name is None:
            raise RuntimeError('File output requires file name to be defined')

        self._dir = dir
        self._file_name = file_name
        self._max_num_files = max_num_files
        super(File, self).__init__(severity)

    def _get_file_name(self, file_name):
        if not file_name.endswith('.log'):
            return '{0}.log'.format(file_name)
        else:
            return file_name
