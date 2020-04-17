import re

def clean(s): 
    if (isinstance(s, float)):
        return str(s) 
    # Replaces url's with < url >
    string = re.sub(r'http\S+', '< url >', s)
    
    # Replaces integers with <number>
    string = re.sub(r'\b\d+', ' <number> ', string)
    
    # Make lowercase and deletes newline
    string = s.lower().replace('\n\n', '')

    # Spaces between acceptable ASCII chars
    string = re.sub(r'([\x21-\x2f\x3a-\x60\x7b-\x7e])', r' \1', string)
    
    # Removes extended ASCII and unicode chars
    string = re.sub(r'[\u2000-\u2027\x80-\xff]', '', string)
    
    # Removes subsequent spaces
    string = re.sub(' +', ' ', string)
    
    string = string.strip()
    
    return string