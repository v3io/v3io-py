import functools


class Session(object):

    def __init__(self, context, access_key):
        self._context = context
        self._access_key = access_key

        for function_name in [
            'get_object',
            'put_object',
            'delete_object',
            'put_item',
            'put_items',
            'update_item',
            'get_item',
            'get_items'
        ]:
            function_implementation = functools.partial(self._call_context_function, function_name)

            setattr(self, function_name, function_implementation)

    def _call_context_function(self, context_function, *args, **kw_args):

        # inject access key in the args
        args_with_access_key = list(args)
        args_with_access_key.insert(1, self._access_key)

        return getattr(self._context, context_function)(*args_with_access_key, **kw_args)
