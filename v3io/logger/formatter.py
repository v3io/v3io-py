import logging
import datetime
import json

import colorama

import v3io.logger


class HumanReadable(logging.Formatter):

    # Maps severity to its letter representation
    _level_to_short_name = {
        v3io.logger.Severity.Debug: 'D',
        v3io.logger.Severity.Info: 'I',
        v3io.logger.Severity.Warning: 'W',
        v3io.logger.Severity.Error: 'E'
    }

    # Maps severity to its color representation
    _level_to_color = {
        v3io.logger.Severity.Warning: colorama.Fore.LIGHTYELLOW_EX,
        v3io.logger.Severity.Error: colorama.Fore.LIGHTRED_EX
    }

    def __init__(self, enable_colors, *args, **kwargs):
        super(HumanReadable, self).__init__(*args, **kwargs)
        self._enable_colors = enable_colors

    def format(self, record):
        def _get_what_color():
            return {
                v3io.logger.Severity.Debug: colorama.Fore.LIGHTCYAN_EX,
                v3io.logger.Severity.Info: colorama.Fore.CYAN,
                v3io.logger.Severity.Warning: colorama.Fore.LIGHTCYAN_EX,
                v3io.logger.Severity.Error: colorama.Fore.LIGHTCYAN_EX,
            }.get(record.levelno, colorama.Fore.LIGHTCYAN_EX)

        output = {
            'reset_color': colorama.Fore.RESET,

            'when': datetime.datetime.fromtimestamp(record.created).strftime('%d.%m.%y %H:%M:%S.%f'),
            'when_color': colorama.Fore.WHITE,

            'who': record.name[-30:],
            'who_color': colorama.Fore.WHITE,

            'severity': HumanReadable._level_to_short_name[record.levelno],
            'severity_color': HumanReadable._level_to_color.get(record.levelno, colorama.Fore.RESET),

            'what': record.getMessage(),
            'what_color': _get_what_color(),

            'more': Json.format_to_json_str(record.vars) if len(record.vars) else '',
            'more_color': colorama.Fore.WHITE,
        }

        # Disable coloring if requested
        if not self._enable_colors:
            for ansi_color in [f for f in output.keys() if 'color' in f]:
                output[ansi_color] = ''

        return '{when_color}{when}{reset_color} {who_color}{who:>30}:{reset_color} ' \
               '{severity_color}({severity}){reset_color} {what_color}{what}{reset_color} ' \
               '{more_color}{more}{reset_color}'.format(**output)


class Json(logging.Formatter):

    class ObjectEncoder(json.JSONEncoder):

        def default(self, obj):
            try:
                return obj.__log__()
            except:
                return obj.__repr__()

    @staticmethod
    def format_to_json_str(params):
        try:

            # default encoding is utf8
            return json.dumps(params, cls=Json.ObjectEncoder)
        except:

            # this is the widest complementary encoding found
            return json.dumps(params, cls=Json.ObjectEncoder, encoding='raw_unicode_escape')

    def format(self, record):
        params = {
            'datetime': self.formatTime(record, self.datefmt),
            'name': record.name,
            'level': record.levelname.lower(),
            'message': record.getMessage()
        }

        params.update(record.vars)

        return Json.format_to_json_str(params)
