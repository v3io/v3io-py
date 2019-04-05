import logging
import colorama


class Severity(object):
    Debug = logging.DEBUG
    Info = logging.INFO
    Warning = logging.WARNING
    Error = logging.ERROR

    @staticmethod
    def get_level_by_string(severity_string):
        string_enum_dict = {
            'debug': Severity.Debug,
            'info': Severity.Info,
            'warn': Severity.Warning,
            'warning': Severity.Warning,
            'error': Severity.Error,
        }

        return string_enum_dict.get(severity_string, 0)


class Client(object):

    # defaults
    class Defaults(object):

        class Stdout(object):
            severity = 'debug'
            colors = 'on'

        class FileRotated(object):
            severity = 'debug'
            max_num_files = 5
            max_file_size = 5

        class FileTimed(object):
            severity = 'debug'
            max_num_files = 56  # To keep our logs under 100GB, we decrease the debug logs to last 14 days
            period = 6 * 60 * 60  # 6 hours

    def __init__(self, name, initial_severity, outputs=None):
        import v3io.logger.output

        # by default, only output to stdout
        outputs = outputs or [
            v3io.logger.output.Stdout('debug', Client.Defaults.Stdout.colors)
        ]

        colorama.init()

        # initialize root logger
        logging.setLoggerClass(_VariableLogging)
        self.logger = logging.getLogger(name)
        self.logger.setLevel(initial_severity)

        if not isinstance(outputs, list):
            outputs = [outputs]

        # tell each output to create the appropriate handler
        for output in outputs:
            output.create_handler(self.logger)

    @staticmethod
    def _get_name(specified_name, name, severity=None):
        if specified_name:
            return specified_name

        return '{0}_{1}'.format(name.replace('-', '.'), severity)


class _VariableLogging(logging.Logger):
    get_child = logging.Logger.getChild

    def __init__(self, name, level=logging.NOTSET):
        logging.Logger.__init__(self, name, level)
        self._bound_variables = {}

    def _check_and_log(self, level, msg, args, kw_args):
        if self.isEnabledFor(level):
            kw_args.update(self._bound_variables)
            self._log(level, msg, args, extra={'vars': kw_args})

    def error_with(self, msg, *args, **kw_args):
        self._check_and_log(Severity.Error, msg, args, kw_args)

    def warn_with(self, msg, *args, **kw_args):
        self._check_and_log(Severity.Warning, msg, args, kw_args)

    def info_with(self, msg, *args, **kw_args):
        self._check_and_log(Severity.Info, msg, args, kw_args)

    def debug_with(self, msg, *args, **kw_args):
        self._check_and_log(Severity.Debug, msg, args, kw_args)

    def bind(self, **kw_args):
        self._bound_variables.update(kw_args)
