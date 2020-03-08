import subprocess
import os
import shlex


def run_as_user(username, command):
    cmd = ("sudo -H -u {user} "
    "bash -c \'{command}\'")
    
    cmd_concrete = cmd.format(user=username,
    command=command)
    print(cmd_concrete)
    status = subprocess.call(cmd_concrete, shell=True)
    

def run(command):
    print(command)
    subprocess.call(command, shell=True)

def set_java():
    def install():
        run_as_user("root", "DEBIAN_FRONTEND=noninteractive apt-get --yes --allow-downgrades install openjdk-9-jdk-headless")

    def configure_java():
        run("echo 'export JAVA_HOME=/usr/lib/jvm/java-9-openjdk-amd64/' >> ~/.bashrc")

    install()
    configure_java()

def set_timezone():
    def install():
        run_as_user("root", "DEBIAN_FRONTEND=noninteractive apt-get --yes --allow-downgrades install timedatectl")

    def timezone():    
        run_as_user("root", "timedatectl set-timezone Asia/Kolkata")

    install()
    timezone()
    
def set_hadoop():
    def install():
        run("mkdir -p downloads")
        run("cd downloads && wget http://mirrors.estointernet.in/apache/hadoop/common/hadoop-3.2.1/hadoop-3.2.1.tar.gz")
        run("mkdir -p programfiles")
        run("cd programfiles && tar -zxvf ../downloads/hadoop-3.2.1.tar.gz")

    def configure_bashrc():
        run("echo 'export HADOOP_HOME=$HOME/programfiles/hadoop-3.2.1/' >> ~/.bashrc")
        run("echo 'export HADOOP_MAPRED_HOME=$HADOOP_HOME' >> ~/.bashrc")
        run("echo 'export HADOOP_COMMON_HOME=$HADOOP_HOME' >> ~/.bashrc")
        run("echo 'export HADOOP_HDFS_HOME=$HADOOP_HOME' >> ~/.bashrc")
        run("echo 'export YARN_HOME=$HADOOP_HOME' >> ~/.bashrc")
        run("echo 'export HADOOP_COMMON_LIB_NATIVE_DIR=$HADOOP_HOME/lib/native' >> ~/.bashrc")
        run("echo 'export PATH=$PATH:$HADOOP_HOME/sbin:$HADOOP_HOME/bin' >> ~/.bashrc")
        run("echo 'export HADOOP_INSTALL=$HADOOP_HOME' >> ~/.bashrc")
        run("echo 'export HADOOP_CLASSPATH=${JAVA_HOME}/lib/tools.jar' >> ~/.bashrc")
        run("echo 'export PATH=${JAVA_HOME}/bin:${PATH}' >> ~/.bashrc")

    install()
    configure_bashrc()

def set_anaconda():
    def install():
        run("cd downloads && wget https://repo.anaconda.com/archive/Anaconda3-2019.10-Linux-x86_64.sh")
        run("bash downloads/Anaconda3-2019.10-Linux-x86_64.sh -b -p $HOME/anaconda3")

    def configure():
        base_string = """
# >>> conda initialize >>>
# !! Contents within this block are managed by 'conda init' !!
__conda_setup="$('/home/azureuser/anaconda3/bin/conda' 'shell.bash' 'hook' 2> /dev/null)"
if [ $? -eq 0 ]; then
    eval "$__conda_setup"
else
    if [ -f "/home/azureuser/anaconda3/etc/profile.d/conda.sh" ]; then
        . "/home/azureuser/anaconda3/etc/profile.d/conda.sh"
    else
        export PATH="/home/azureuser/anaconda3/bin:$PATH"
    fi
fi
unset __conda_setup
# <<< conda initialize <<<
"""
        conda_conf = base_string.replace('azureuser', os.environ['USER'])
        with open("{}/.bashrc".format(os.environ['HOME']), "a") as f:
            f.write(conda_conf)
            # print(f.read())

    install()
    configure()

def main():
    # run("echo 123 > /tmp/user.txt")
    # run_as_user("root", "echo 123 > /tmp/toot.txt")           
    # set_java()
    # set_timezone()
    # set_hadoop()
    # set_anaconda()



if __name__ == "__main__":
    main()
