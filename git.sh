#!/bin/bash

set -euo pipefail

BRANCH="aiot-3588ied"
REMOTE="origin"
BATCHES=5  # 拆分批次数，超限则改大

# 1. 前置准备：启用协议v2 + 强制获取远程引用
git config protocol.version 2
echo ">>> Fetching remote refs..."
git fetch "$REMOTE" || true  # 即使远程无此分支也不中断

# 2. 安全计算未推送提交数（兼容远程分支不存在的情况）
if git rev-parse --verify "$REMOTE/$BRANCH" &>/dev/null; then
    RANGE="$REMOTE/$BRANCH..$BRANCH"
else
    echo ">>> Remote branch $BRANCH not found, treating as new branch"
    RANGE="$BRANCH"
fi

TOTAL=$(git rev-list --count "$RANGE")
if [ "$TOTAL" -eq 0 ]; then
    echo ">>> Already up to date."
    git branch --set-upstream-to="$REMOTE/$BRANCH" "$BRANCH" 2>/dev/null || true
    exit 0
fi

STEP=$(( (TOTAL + BATCHES - 1) / BATCHES ))  # 向上取整，避免漏推
echo ">>> Total: $TOTAL commits, splitting into $BATCHES batches (~$STEP per batch)"

# 3. 循环分批推送
for i in $(seq "$STEP" "$STEP" "$TOTAL"); do
    ANCHOR=$(git rev-list --reverse "$RANGE" | sed -n "${i}p")
    echo ">>> Pushing batch at commit #$i / $TOTAL (${ANCHOR:0:10})"
    git push "$REMOTE" "$ANCHOR:refs/heads/$BRANCH" || {
        echo "!!! Batch failed at #$i. Re-run after increasing BATCHES value."
        exit 1
    }
done

# 4. 推送剩余尾批 + 绑定上游
echo ">>> Pushing final batch and setting upstream..."
git push --set-upstream "$REMOTE" "$BRANCH"
echo ">>> Done ✅"
