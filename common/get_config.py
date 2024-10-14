class Config:
    def __init__(self):
        self.config = self.get_config()

    def get_config(self):
        config = {}
        with open('./.config', 'r', encoding='utf-8') as f:
            lines = f.readlines()
        for line in lines:
            line = list(str(line))
            if '#' in line:
                line = line[:line.index('#')]
            line = ''.join(line).strip(' \n')
            if '=' in line:
                key, value = line.split('=')
                key = key.strip(' ')
                value = value.strip(' \'\"')
                config[key] = value
        return config
    
    def get_value(self, key):
        return self.config[key]

    def __getitem__(self, key):
        return self.config[key]

if __name__ == '__main__':
    cf = Config()
    print(cf.config)
