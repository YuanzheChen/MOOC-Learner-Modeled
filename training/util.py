
def initialize_form_dicts(form_dicts):
    try:
        for element_dict in form_dicts:
            element_dict['value'] = element_dict['default']
        return form_dicts
    except KeyError:
        raise ValueError("Invalid form dictionary")


def preprocess_input(s):
    try:
        s = int(s)
        return s
    except ValueError:
        pass
    try:
        s = float(s)
        return s
    except ValueError:
        pass
    try:
        s = str(s)
        return s
    except ValueError:
        pass
    return s


def process_form_input(form_type, form_dicts, form_input):
    for element_dict in form_dicts:
        element_id = element_dict['type'] + '_' + form_type + '_' + element_dict['name']
        if (element_id + '_value') in form_input:
            element_dict['value'] = preprocess_input(form_input[element_id + '_value'])
    return form_dicts


def form_dicts_to_dict(form_dicts):
    form_dict = dict()
    for element_dict in form_dicts:
        form_dict[element_dict['name']] = {k: element_dict[k]
                                           for k in element_dict
                                           if k != 'name'}
    return form_dict