import platform

DATA_FOLDER = ""
if platform.system().lower() == 'windows':
    DATA_FOLDER = 'D:\\Custom_Files\\大四上\\毕业设计\\参考项目\\citation_lda\\data\\dblp'
elif platform.system().lower() == 'linux':
    DATA_FOLDER = '/home/zhengronggui/test/citation_lda/data/dblp'

THEME = "Environmental Science"
THEME_PATH='Environmental_Science'

def change_theme(name):
    global THEME
    THEME= name
    print(THEME)