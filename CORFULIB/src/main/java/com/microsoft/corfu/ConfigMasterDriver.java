/**
 * Copyright (C) 2014 Microsoft Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.microsoft.corfu;

import java.io.FileNotFoundException;

/**
 * Created by dalia on 7/1/2014.
 */
public class ConfigMasterDriver {
    public static void main(String[] args) throws CorfuException, FileNotFoundException, InterruptedException {
        Runnable t;
        if (args.length > 0 && "-recover".equals(args[0]))
            t = new ConfigMasterService("./corfu.xml", true);
        else
            t = new ConfigMasterService("./corfu.xml", false);

        new Thread(t).start();
    }
}
