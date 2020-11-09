import os

try:
    os.mkdir('/usr/lib/python3/dist-packages/envcanlib')
except OSError:
    os.system('rm -rf /usr/lib/python3/dist-packages/envcanlib/*')

os.system('cp -r ./* /usr/lib/python3/dist-packages/envcanlib')

asw = input('Would you like to install it in Anaconda as well? [Y/n]: ')
if asw  == 'Y' or asw == 'y':
    path = input('Tell us your anaconda path:')
    try:
        os.mkdir(path+'/lib/python3*/site-packages/envcanlib')
    except OSError:
        os.system('rm -rf '+path+'/lib/python3*/site-packages/envcanlib/*')
    os.system('cp -r ./* '+path+'/lib/python3*/site-packages/envcanlib/')

print('Installation has finished')
