#!/bin/sh
# The runner image's entry point: restricts the container's traffic, then starts `dtpipe node --runner` as 1654:1654 with no
# capability. deploy/README.md says what a deployment sets.
#
#   DTPIPE_EGRESS_ALLOW   CIDR masks, comma-separated, IPv4 or IPv6, a host written /32 or /128: the only addresses the runner
#                         may open a connection to, beside the loopback. Empty: nothing is restricted, and a warning says so.
#   DTPIPE_EGRESS_RULES   on (the default) or off. Off installs nothing, for a deployment whose network restricts the runner
#                         (a Kubernetes NetworkPolicy); a warning says so.
#
# With a list, the start fails unless every rule is installed and read back: a runner that cannot apply its list never runs as
# though it did. The rules, for IPv4, and for IPv6 whenever the kernel has it: every chain drops by default; the loopback is open
# both ways (`arrow:` reaches the node's pipes there, and Docker's resolver listens at 127.0.0.11); so are the replies to a
# connection the runner opened, and IPv6 neighbour discovery; out, the masks; any other connection out is rejected, so it fails
# at once rather than after a timeout, and any other ICMP dropped. Nothing comes in but those replies.
#
# Root is needed only here, with NET_ADMIN for the rules, to read a key file that is root's alone, and with SETUID, SETGID and
# SETPCAP to leave it: dtpipe runs with every capability dropped, the bounding set emptied and no_new_privs set, so it can
# neither change the rules nor gain a privilege back. Started as another user, the script installs nothing and starts dtpipe as
# it is, which a non-empty list refuses.
set -eu

worker=1654

error() { echo "[ERROR] $*" >&2; exit 1; }
warn() { echo "[WARN] $*" >&2; }

allow=$(printf '%s' "${DTPIPE_EGRESS_ALLOW:-}" | tr -d ' \t\r\n')
rules=${DTPIPE_EGRESS_RULES:-on}
case "$rules" in
    on | off) ;;
    *) error "DTPIPE_EGRESS_RULES is '$rules': it takes on or off." ;;
esac

v4=''
v6=''
if [ -n "$allow" ]; then
    case "$allow" in
        ,* | *, | *,,*) error "DTPIPE_EGRESS_ALLOW has an empty entry: masks are separated by single commas." ;;
    esac
    old_ifs=$IFS
    IFS=,
    for mask in $allow; do
        if printf '%s' "$mask" | grep -Eq '^((25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])\.){3}(25[0-5]|2[0-4][0-9]|1[0-9][0-9]|[1-9]?[0-9])/(3[0-2]|[12]?[0-9])$'; then
            v4="$v4 $mask"
        elif printf '%s' "$mask" | grep -Eq '^[0-9A-Fa-f:.]*:[0-9A-Fa-f:.]*/(12[0-8]|1[01][0-9]|[1-9]?[0-9])$'; then
            v6="$v6 $mask"
        else
            error "DTPIPE_EGRESS_ALLOW: '$mask' is not a CIDR mask (an address and a prefix length; a host is written /32 or /128)."
        fi
    done
    IFS=$old_ifs
fi

off_warning="DTPIPE_EGRESS_RULES=off: the runner installs no network rules${allow:+ and DTPIPE_EGRESS_ALLOW is not applied}; its traffic must be restricted around it, by a Kubernetes NetworkPolicy or an equivalent."
empty_warning="DTPIPE_EGRESS_ALLOW is empty: the runner's traffic is not restricted. Set it to the CIDR masks of what the runner reaches (its coordinator, its identity provider), comma-separated."

if [ "$(id -u)" != 0 ]; then
    if [ "$rules" = off ]; then
        warn "$off_warning"
    elif [ -n "$allow" ]; then
        error "DTPIPE_EGRESS_ALLOW is set but the runner started as uid $(id -u): its rules need root and NET_ADMIN. Start the image as root, its default, with NET_ADMIN, or set DTPIPE_EGRESS_RULES=off where the network restricts the runner."
    else
        warn "$empty_warning"
    fi
    exec dtpipe node --runner "$@"
fi

# CapEff bits: SETGID 6, SETUID 7, SETPCAP 8, NET_ADMIN 12.
effective=$(sed -n 's/^CapEff:[[:space:]]*//p' /proc/self/status)
missing=''
need='SETUID:7 SETGID:6 SETPCAP:8'
[ "$rules" = off ] || [ -z "$allow" ] || need="NET_ADMIN:12 $need"
for cap in $need; do
    [ $(((0x$effective >> ${cap#*:}) & 1)) -eq 1 ] || missing="$missing ${cap%:*}"
done
[ -z "$missing" ] || error "started as root, the runner needs NET_ADMIN to install its rules and SETUID, SETGID and SETPCAP to start dtpipe as $worker with no capability; it lacks:$missing. Add them to the container (cap_add), or start it as another user with DTPIPE_EGRESS_RULES=off."

# $1: iptables or ip6tables; $2: -A to install, -C to check; $3: the masks. The same list does both, so what is checked is what
# was installed.
apply() {
    tool=$1 op=$2 masks=$3
    rule INPUT -i lo -j ACCEPT
    rule INPUT -m conntrack --ctstate ESTABLISHED,RELATED -j ACCEPT
    rule OUTPUT -o lo -j ACCEPT
    rule OUTPUT -m conntrack --ctstate ESTABLISHED,RELATED -j ACCEPT
    if [ "$tool" = ip6tables ]; then
        for chain in INPUT OUTPUT; do
            rule "$chain" -p ipv6-icmp --icmpv6-type neighbour-solicitation -j ACCEPT
            rule "$chain" -p ipv6-icmp --icmpv6-type neighbour-advertisement -j ACCEPT
        done
    fi
    for mask in $masks; do
        rule OUTPUT -d "$mask" -j ACCEPT
    done
    # ICMP outside the list falls to the policy: the kernel's own IPv6 router solicitations and multicast reports are not
    # connections, and the REJECT rule counts nothing else.
    if [ "$tool" = ip6tables ]; then
        rule OUTPUT -p ipv6-icmp -j DROP
        rule OUTPUT -j REJECT --reject-with icmp6-adm-prohibited
    else
        rule OUTPUT -p icmp -j DROP
        rule OUTPUT -j REJECT --reject-with icmp-admin-prohibited
    fi
}

rule() {
    out=$("$tool" -w "$op" "$@" 2>&1) || {
        [ "$op" = -A ] || error "$tool does not hold the rule '$*' it was given: the rules are not as installed."
        error "$tool refused the rule '$*' (DTPIPE_EGRESS_ALLOW): $out"
    }
}

# The policies first, so that a failure part-way leaves everything dropped; the chains are emptied of anything a previous start
# of the same network namespace left.
install() {
    tool=$1
    command -v "$tool" >/dev/null || error "DTPIPE_EGRESS_ALLOW is set and the image has no $tool."
    for chain in INPUT FORWARD OUTPUT; do
        out=$("$tool" -w -P "$chain" DROP 2>&1) || error "$tool cannot set the $chain policy (DTPIPE_EGRESS_ALLOW is set): $out"
        out=$("$tool" -w -F "$chain" 2>&1) || error "$tool cannot empty the $chain chain: $out"
    done
    apply "$tool" -A "$2"
    apply "$tool" -C "$2"
    for chain in INPUT FORWARD OUTPUT; do
        "$tool" -w -S "$chain" 2>/dev/null | grep -qx -- "-P $chain DROP" || error "$tool does not report the $chain policy as DROP."
    done
}

if [ "$rules" = off ]; then
    warn "$off_warning"
elif [ -z "$allow" ]; then
    warn "$empty_warning"
else
    install iptables "$v4"
    if [ -e /proc/net/if_inet6 ]; then
        install ip6tables "$v6"
    elif [ -n "$v6" ]; then
        warn "DTPIPE_EGRESS_ALLOW names IPv6 masks and the kernel has no IPv6: they are not installed, and the runner has no IPv6 traffic."
    fi
fi

# A client secret given as @path is read here, before the privileges go, so its file may be root's alone: dtpipe gets the value in
# its own environment, which its steps neither inherit nor read (a runner is not dumpable).
case "${DtPipeNode__Oidc__ClientSecret:-}" in
    @*)
        secret=$(cat -- "${DtPipeNode__Oidc__ClientSecret#@}" 2>/dev/null) || error "DtPipeNode__Oidc__ClientSecret names a file that cannot be read."
        [ -n "$secret" ] || error "DtPipeNode__Oidc__ClientSecret names an empty file."
        DtPipeNode__Oidc__ClientSecret=$secret
        export DtPipeNode__Oidc__ClientSecret
        unset secret
        ;;
esac

home=$(getent passwd "$worker" | cut -d: -f6)
export HOME="${home:-/tmp}"
exec setpriv --reuid="$worker" --regid="$worker" --clear-groups --bounding-set=-all --inh-caps=-all --no-new-privs \
    -- dtpipe node --runner "$@"
