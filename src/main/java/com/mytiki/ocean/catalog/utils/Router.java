/*
 * Copyright (c) TIKI Inc.
 * MIT license. See LICENSE file in root directory.
 */

package com.mytiki.ocean.catalog.utils;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;

import java.util.HashMap;
import java.util.Map;

public class Router<I,O> {
    private final Map<String, RequestHandler<I,O>> routes = new HashMap<>();

    public Router<I,O> add(String route, RequestHandler<I,O> handler){
        routes.put(route, handler);
        return this;
    }

    public Router<I,O> add(String method, String path, RequestHandler<I,O> handler){
        return add(toRoute(method, path), handler);
    }

    public O handle(String route, final I request, final Context context) {
        return routes.get(route).handleRequest(request, context);
    }

    public O handle(String method, String path, final I request, final Context context) {
        return handle(toRoute(method, path), request, context);
    }

    private String toRoute(String method, String path) {
        return method + " " + path;
    }
}
