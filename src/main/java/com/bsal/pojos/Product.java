package com.bsal.pojos;

import lombok.*;

@AllArgsConstructor
@NoArgsConstructor
@Builder
@Getter
@Setter
public class Product {
    private Integer id;
    private String name;
    private String description;
    private double price;
}