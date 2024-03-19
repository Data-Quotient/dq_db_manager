import json

def extract_details(executor, query, model_class, return_as_dict: bool = False, *params) -> list:
    print(f"EXECUTING: {query}\nPARAMS: {params}")
    raw_data = executor(query, params)
    data_objects = [model_class(**dict(zip(model_class.model_fields, row))) for row in raw_data]
    if return_as_dict:
        return [dict(json.loads(data_object.model_dump_json())) for data_object in data_objects]
    else:
        return data_objects

def add_table_to_query(query, params, table_name, alias="table_name"):
    query += f"AND {alias} = %s"
    params.append(table_name)
    return query, params

def add_index_to_query(query, params, index_name, alias="index_name"):
    query += f"AND {alias} = %s"
    params.append(index_name)
    return query, params

def add_view_to_query(query, params, view_name, alias="view_name"):
    query += f"AND {alias} = %s"
    params.append(view_name)
    return query, params

def add_trigger_to_query(query, params, trigger_name, alias="trigger_name"):
    query += f"AND {alias} = %s"
    params.append(trigger_name)
    return query, params