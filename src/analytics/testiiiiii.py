temp_dict = {'IN': [{'daryltractor': 1}, {'beat': 2}, {'corona': 2}, {'donation': 2}, {'good': 2}, {'sanitiser': 2}, {'look': 2}, {'unemployment': 2}, {'smiling': 2}, {'while': 2}, {'mask': 1}, {'talking': 2}, {'about': 2}, {'deaths': 2}, {'attention': 2}, {'span': 2}, {'gnat': 2}, {'stephasher': 2}, {'aditya': 1}], 'GB': [{'stanneel': 1}, {'beat': 1}, {'good': 1}, {'look': 1}, {'donation': 1}, {'smiling': 1}, {'while': 1}, {'talking': 1}, {'mask': 1}, {'corona': 1}, {'about': 1}, {'deaths': 1}, {'attention': 1}, {'span': 1}, {'gnat': 1}, {'stephasher': 1}]}

new_dict = {'IN': [{'daryltractor': 1}, {'beat': 2}, {'corona': 2}, {'donation': 2}, {'good': 2}, {'sanitiser': 2}, {'look': 2}, {'unemployment': 2}, {'smiling': 2}, {'while': 2}, {'mask': 1}, {'talking': 2}, {'about': 2}, {'deaths': 2}, {'attention': 2}, {'span': 2}, {'gnat': 2}, {'stephasher': 2}, {'aditya': 1}], 'GB': [{'stanneel': 1}, {'beat': 1}, {'good': 1}, {'look': 1}, {'donation': 1}, {'smiling': 1}, {'while': 1}, {'talking': 1}, {'mask': 1}, {'corona': 1}, {'about': 1}, {'deaths': 1}, {'attention': 1}, {'span': 1}, {'gnat': 1}, {'stephasher': 1}]}

for key, value in temp_dict.items():
    if key not in new_dict.keys():
        new_dict[key] = value

    else:
        updated_dict = {}
        old_list = temp_dict[key]
        for dictionary in old_list:
            for key1, val1 in dictionary.items():
                updated_dict[key1] = val1

        new_list = new_dict[key]
        for dictionary in new_list:
            for key1, val1 in dictionary.items():
                updated_dict[key1] = updated_dict.get(key1,0) + val1

        new_updated_list =[]
        for key1, val1 in updated_dict.items():
            new_updated_list.append({key1:val1})

        new_dict[key] = new_updated_list













