/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

use beam::prelude::*;
use beam::testing::{TestPipeline, passert};
use join::JoinOrders;

fn pairs(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
    pairs
        .iter()
        .map(|&(key, value)| (key.to_string(), value.to_string()))
        .collect()
}

/// `u2` has no order and `u3` has no user, so each join keeps or drops them.
#[tokio::test]
async fn joins_users_with_orders() {
    let p = TestPipeline::new();
    let users = p.apply(
        "Users",
        Create::new(pairs(&[("u1", "Alice"), ("u2", "Bob")])),
    );
    let orders = p.apply(
        "Orders",
        Create::new(pairs(&[("u1", "Laptop"), ("u3", "Mouse")])),
    );

    let joined = users.apply("JoinOrders", JoinOrders { orders });

    passert::that("AssertJoined", &joined).contains_in_any_order(
        [
            r#"grouped u1: users=["Alice"] orders=["Laptop"]"#,
            r#"grouped u2: users=["Bob"] orders=[]"#,
            r#"grouped u3: users=[] orders=["Mouse"]"#,
            "inner u1: Alice, Laptop",
            "left u1: Alice, Laptop",
            "left u2: Bob, -",
            "right u1: Alice, Laptop",
            "right u3: -, Mouse",
            "full u1: Alice, Laptop",
            "full u2: Bob, -",
            "full u3: -, Mouse",
        ]
        .map(String::from),
    );

    p.run().await.expect("pipeline and assertions must pass");
}
