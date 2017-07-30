package io.logz.sawmill.processors.ahocorasick;

import org.ahocorasick.trie.Emit;
import org.ahocorasick.trie.Trie;

import java.util.Collection;
import java.util.List;

public class AhoCorasickModel {

    Trie model;

    public Collection<Emit> search(String input) {
        return model.parseText(input);
    }

    public void build(List<String> lines) {
        Trie.TrieBuilder builder = Trie.builder();
        lines.stream().forEach(line -> builder.addKeyword(line));
        model = builder.build();
    }

}
