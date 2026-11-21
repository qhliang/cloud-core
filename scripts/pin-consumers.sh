#!/usr/bin/env bash
#
# 把四个消费者仓的 `cloud-core` / `cloud-core-queue` rev 一起改成同一个提交。
#
# 为什么需要脚本：这两个依赖是**两处独立的声明**（根包 + 队列库），写在
# 五个 Cargo.toml 里（tokengateway 有两个 crate 引用 cloud-core）。只改其中
# 一处会让同一份构建里出现两份 cloud-core 工作区 —— 报错是「同名不同类型」，
# 与真因相距很远。脚本保证：**要么全改，要么一个都不改**。
#
# 用法：
#   ./scripts/pin-consumers.sh                # 钉到当前 HEAD
#   ./scripts/pin-consumers.sh <40 位 sha>    # 钉到指定提交
#
# 约定：`rev` 用 40 位全 sha。短 sha 会被 cargo 当前缀匹配，将来撞前缀时
# 行为不可预测；tag 可以被移动。
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
WORKSPACE_ROOT="$(cd "${REPO_ROOT}/.." && pwd)"

REV="${1:-}"
if [ -z "${REV}" ]; then
    REV="$(git -C "${REPO_ROOT}" rev-parse HEAD)"
fi
if ! printf '%s' "${REV}" | grep -qE '^[0-9a-f]{40}$'; then
    echo "✗ rev 必须是 40 位 sha：${REV}" >&2
    exit 1
fi

# 消费者仓（都与 cloud-core 同级的 monorepo 检出）。缺失的仓跳过 ——
# 只把 cloud-core 单仓 clone 出来的人不应该因此失败。
MANIFESTS=(
    "agent-manager/Cargo.toml"
    "workflow-manager/Cargo.toml"
    "cloud_manager/cloud-manager-core/Cargo.toml"
    "tokengateway/crates/core-types/Cargo.toml"
    "tokengateway/crates/tokengateway/Cargo.toml"
    "gateway-proxy/Cargo.toml"
)

PATTERN='(git = "https://github\.com/qhliang/cloud-core\.git", rev = ")[0-9a-f]{40}(")'

changed=0
for rel in "${MANIFESTS[@]}"; do
    file="${WORKSPACE_ROOT}/${rel}"
    if [ ! -f "${file}" ]; then
        echo "· 跳过（不存在）：${rel}"
        continue
    fi
    if ! grep -qE "${PATTERN}" "${file}"; then
        echo "✗ ${rel} 里没有「git + rev」形式的 cloud-core 依赖 —— 先手工补上 rev 再跑本脚本" >&2
        exit 1
    fi
    # perl 在 macOS / Linux 都在，避免 sed -i 的方言差异。
    PERL_REV="${REV}" perl -pi -e \
        's{(git = "https://github\.com/qhliang/cloud-core\.git", rev = ")[0-9a-f]{40}(")}{$1$ENV{PERL_REV}$2}g' \
        "${file}"
    echo "✓ ${rel}"
    changed=$((changed + 1))
done

if [ "${changed}" -eq 0 ]; then
    echo "✗ 一个消费者都没找到 —— 确认本仓与消费者仓是同级目录" >&2
    exit 1
fi

echo
echo "刷新各仓 Cargo.lock …"
for rel in "${MANIFESTS[@]}"; do
    repo_dir="${WORKSPACE_ROOT}/$(dirname "${rel}")"
    # 从**仓根**跑 cargo（workspace 根不一定是 manifest 所在目录）。
    while [ "${repo_dir}" != "${WORKSPACE_ROOT}" ] && [ ! -f "${repo_dir}/Cargo.toml" ]; do
        repo_dir="$(dirname "${repo_dir}")"
    done
    [ -f "${repo_dir}/Cargo.toml" ] || continue
    if (cd "${repo_dir}" && cargo metadata --format-version 1 >/dev/null 2>&1); then
        echo "✓ $(basename "${repo_dir}")"
    else
        echo "✗ $(basename "${repo_dir}") 解析失败 —— 确认 ${REV} 已 push 到 origin" >&2
        exit 1
    fi
done

echo
echo "完成：${REV}"
echo "下一步：分别在四个仓 cargo fmt / clippy / test，再提交 Cargo.toml + Cargo.lock。"
