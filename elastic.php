<?php
    'query' => [
        'match_all' => (object) []
    ]

    'query' => [
        'match' => [
            'account_number' => '20'
        ]
    ]

    'query' => [
        "bool": {
            "must": [
                {
                    "match": {
                        "address": "mill"
                    }
                },
                {
                    "match": {
                        "address": "lane"
                    }
                }
            ],
            "should": [
                {
                    "match": {
                        "address": "mill"
                    }
                },
                {
                    "match": {
                        "address": "lane"
                    }
                }
            ]
        }
    ]

?>