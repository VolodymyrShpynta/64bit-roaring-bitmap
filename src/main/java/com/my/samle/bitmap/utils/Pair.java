package com.my.samle.bitmap.utils;

import lombok.*;

/**
 * Created by Volodymyr Shpynta on 24.10.16.
 */
@Getter
@Setter
@EqualsAndHashCode
@NoArgsConstructor
@AllArgsConstructor
public class Pair<K,V> {
    private K key;
    private V value;
}
