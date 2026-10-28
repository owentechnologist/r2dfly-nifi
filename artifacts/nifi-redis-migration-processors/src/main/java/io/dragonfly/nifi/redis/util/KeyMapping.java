package io.dragonfly.nifi.redis.util;

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.processor.ProcessContext;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Source key to target key: the longest matching rename rule swaps one prefix for another, then
 * the Key Prefix, when set, goes in front. Every processor that writes, deletes, compares or
 * indexes target keys maps through this one type, so they all agree on each key's target name.
 *
 * <p>The mapping is kept one to one. No rule's new prefix may start another's, so a target key
 * names at most one rule, and a source key whose target name a renamed key would also claim (a
 * source key already under a new prefix) has no target name at all: {@link #toTarget} answers
 * empty and the caller reports it rather than letting two keys overwrite each other.
 */
public final class KeyMapping {

    public static final PropertyDescriptor KEY_RENAME = new PropertyDescriptor.Builder()
            .name("key-prefix-rename")
            .displayName("Key Prefix Rename")
            .description("Comma-separated old=new prefix rules, e.g. user:=account:,session:=s: . A key starting with "
                    + "an old prefix is written under the new one instead, and the longest matching old prefix wins. An "
                    + "empty old prefix matches every key. A source key already under a new prefix would collide with a "
                    + "renamed key, so it is reported as a failure instead of written. Applied before Key Prefix.")
            .required(false)
            .addValidator(new RulesValidator())
            .build();

    private record Rule(String from, String to) {
    }

    private static final KeyMapping IDENTITY = new KeyMapping(List.of(), "");

    /** Longest old prefix first, so the first match is the longest. */
    private final List<Rule> rules;
    private final String outerPrefix;

    private KeyMapping(List<Rule> rules, String outerPrefix) {
        this.rules = rules;
        this.outerPrefix = outerPrefix;
    }

    public static KeyMapping identity() {
        return IDENTITY;
    }

    /** {@code prefix} and {@code separator} are the older Key Prefix properties: prefix + separator goes in front. */
    public static KeyMapping of(String renameRules, String prefix, String separator) {
        String outer = prefix == null || prefix.isEmpty() ? "" : prefix + (separator == null ? "" : separator);
        List<Rule> rules = parse(renameRules);
        return rules.isEmpty() && outer.isEmpty() ? IDENTITY : new KeyMapping(rules, outer);
    }

    /** From a processor that has the rename property and, optionally, the two Key Prefix ones. */
    public static KeyMapping of(ProcessContext context, PropertyDescriptor prefix, PropertyDescriptor separator) {
        return of(context.getProperty(KEY_RENAME).getValue(),
                prefix == null ? null : context.getProperty(prefix).getValue(),
                separator == null ? null : context.getProperty(separator).getValue());
    }

    public boolean isIdentity() {
        return this == IDENTITY;
    }

    public Optional<String> toTarget(String sourceKey) {
        Rule rule = ruleFor(sourceKey);
        if (rule == null && claimedByRename(sourceKey)) {
            return Optional.empty();
        }
        String renamed = rule == null ? sourceKey : rule.to + sourceKey.substring(rule.from.length());
        return Optional.of(outerPrefix + renamed);
    }

    /** The source key that maps to {@code targetKey}; empty when no source key does. */
    public Optional<String> toSource(String targetKey) {
        if (!targetKey.startsWith(outerPrefix)) {
            return Optional.empty();
        }
        String renamed = targetKey.substring(outerPrefix.length());
        Rule rule = rules.stream().filter(r -> renamed.startsWith(r.to)).findFirst().orElse(null);
        String source = rule == null ? renamed : rule.from + renamed.substring(rule.to.length());
        return toTarget(source).filter(targetKey::equals).map(t -> source);
    }

    /**
     * The target prefixes that hold every key a source prefix covers, for an index's PREFIX list:
     * the prefix mapped, plus the new prefix of each rule whose old prefix the source prefix covers.
     */
    public List<String> targetPrefixes(String sourcePrefix) {
        Set<String> prefixes = new LinkedHashSet<>();
        Rule rule = ruleFor(sourcePrefix);
        prefixes.add(outerPrefix + (rule == null ? sourcePrefix : rule.to + sourcePrefix.substring(rule.from.length())));
        for (Rule r : rules) {
            if (r.from.startsWith(sourcePrefix) && r.from.length() > sourcePrefix.length()) {
                prefixes.add(outerPrefix + r.to);
            }
        }
        return new ArrayList<>(prefixes);
    }

    private Rule ruleFor(String sourceKey) {
        return rules.stream().filter(r -> sourceKey.startsWith(r.from)).findFirst().orElse(null);
    }

    /** A key no rule renames, sitting where a renamed key would land. */
    private boolean claimedByRename(String sourceKey) {
        return rules.stream().anyMatch(r -> sourceKey.startsWith(r.to));
    }

    private static List<Rule> parse(String value) {
        List<Rule> rules = new ArrayList<>();
        if (value == null || value.isBlank()) {
            return rules;
        }
        for (String part : value.split(",")) {
            if (part.isBlank()) {
                continue;
            }
            int eq = part.indexOf('=');
            if (eq < 0) {
                throw new IllegalArgumentException("'" + part.trim() + "' is not old=new");
            }
            Rule rule = new Rule(part.substring(0, eq).trim(), part.substring(eq + 1).trim());
            // Stripping a prefix would land renamed keys anywhere in the keyspace, so no key could be told apart.
            if (rule.to.isEmpty()) {
                throw new IllegalArgumentException("'" + part.trim() + "' has no new prefix");
            }
            rules.add(rule);
        }
        for (int i = 0; i < rules.size(); i++) {
            for (int j = 0; j < rules.size(); j++) {
                if (i == j) {
                    continue;
                }
                if (rules.get(i).from.equals(rules.get(j).from)) {
                    throw new IllegalArgumentException("the old prefix '" + rules.get(i).from + "' appears twice");
                }
                if (rules.get(j).to.startsWith(rules.get(i).to)) {
                    throw new IllegalArgumentException("the new prefixes '" + rules.get(i).to + "' and '" + rules.get(j).to
                            + "' overlap, so a target key could come from either rule");
                }
            }
        }
        rules.sort(Comparator.comparingInt((Rule r) -> r.from.length()).reversed());
        return rules;
    }

    private static final class RulesValidator implements Validator {
        @Override
        public ValidationResult validate(String subject, String input, org.apache.nifi.components.ValidationContext context) {
            try {
                parse(input);
                return new ValidationResult.Builder().subject(subject).input(input).valid(true).build();
            } catch (IllegalArgumentException e) {
                return new ValidationResult.Builder().subject(subject).input(input).valid(false).explanation(e.getMessage()).build();
            }
        }
    }
}
