def convert_url_to_vis_string(expanded_url):
    """Input an expanded Looker Visualisation URL and this will print
    the relevant url param to generate a custom drill down.
    Note that this function prints the output rather than returning it"""
    from urllib.parse import urlparse, unquote_plus as unquote

    parsed_url = urlparse(expanded_url)
    query_components = unquote(parsed_url.query).split('&')
    query_dict = {}
    has_filter_config = False
    filter_config = ''  # Initialize
    filter_config_url = ''  # Initialize
    for c in query_components:
        if c[:13] == 'filter_config':
            has_filter_config = True
            filter_config = c[14:].replace("'", '"').replace('"', '\\"')
            filter_config_url = "&filter_config={{ filter_config | encode_uri }}"
        else:
            c = c.split('=')
            query_dict[c[0]] = c[1]

    vis_string = query_dict['vis'].replace("'", '"').replace('"', '\\"')
    dynamic_fields = ''  # Initialize
    try:
        dynamic_fields = query_dict['dynamic_fields']
        dynamic_fields = dynamic_fields.replace("'", '"').replace('"', '\\"')
        table_calc_url = "&dynamic_fields={{ table_calc | replace: '  ', '' | encode_uri }}"
        has_table_calc = True
    except KeyError:
        has_table_calc = False
        table_calc_url = ''

    ignores = ['fields', 'vis', 'origin', 'dynamic_fields']
    other_params = [
        "&{}={}".format(k, v.replace("'", '"').replace('"', '\\"'))
        for k, v in query_dict.items() if k not in ignores and k[:2] != 'f[' and v != ''
    ]

    print("url: \"")
    if has_table_calc:
        print("{{% assign table_calc = '{}' %}} ".format(dynamic_fields))
    if has_filter_config:
        print("{{% assign filter_config = '{}' %}} ".format(filter_config))
    print("{% assign vis_config = '")
    lines = vis_string.split(',')
    for line in lines[:-1]:
        print("\t{} ,".format(line))
    print("\t{}' %}}\n\n{{{{ link }}}}&vis_config={{{{ vis_config | encode_uri }}}}"
        "{}{}{}\"".format(lines[-1], ''.join(other_params), filter_config_url, table_calc_url))
