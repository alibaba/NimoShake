#!/bin/bash

set -o errexit

# older version Git don't support --short !
if [ -d ".git" ];then
    #branch=`git symbolic-ref --short -q HEAD`
    branch=$(git symbolic-ref -q HEAD | awk -F'/' '{print $3;}')
    cid=$(git rev-parse HEAD)
else
    branch="unknown"
    cid="0.0"
fi
branch=$branch","$cid

output=./bin/
rm -rf ${output}

# make sure we're in the directory where the script lives
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

GOPATH=$(pwd)
export GOPATH

# golang version
goversion=$(go version | awk -F' ' '{print $3;}')
bigVersion=$(echo $goversion | awk -F'[o.]' '{print $2}')
midVersion=$(echo $goversion | awk -F'[o.]' '{print $3}')
if  [ $bigVersion -lt "1" -o $bigVersion -eq "1" -a $midVersion -lt "6" ]; then
    echo "go version[$goversion] must >= 1.6"
    exit 1
fi

t=$(date "+%Y-%m-%d_%H:%M:%S")

echo "[ BUILD RELEASE ]"
run_builder='go build -v'

modules=(nimo-shake nimo-full-check)
#modules=(nimo-shake)

goos=(linux darwin)
for g in "${goos[@]}"; do
    export GOOS=$g
    echo "try build goos=$g"
    for i in "${modules[@]}" ; do
        echo "build "$i
        info="$i/common.Version=$branch"
        info=$info","$goversion
        info=$info","$t
        $run_builder -ldflags "-X $info" -o "${output}/$i.$g" "./src/$i/main/main.go"
        echo "build $i successfully!"
    done
    echo "build goos=$g: all modules successfully!"
done

# copy scripts
cp scripts/start.sh ${output}/
cp scripts/stop.sh ${output}/
