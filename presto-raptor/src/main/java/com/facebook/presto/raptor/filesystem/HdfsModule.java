/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.raptor.filesystem;

import com.facebook.presto.raptor.storage.OrcDataEnvironment;
import com.facebook.presto.raptor.storage.StorageManagerConfig;
import com.facebook.presto.raptor.storage.StorageService;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import org.apache.hadoop.fs.Path;

import javax.inject.Singleton;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;

public class HdfsModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        configBinder(binder).bindConfig(StorageManagerConfig.class);
        configBinder(binder).bindConfig(RaptorHdfsConfig.class);
        binder.bind(RaptorHdfsConfiguration.class).to(RaptorHiveHdfsConfiguration.class).in(Scopes.SINGLETON);
        binder.bind(StorageService.class).to(HdfsStorageService.class).in(Scopes.SINGLETON);
        binder.bind(OrcDataEnvironment.class).to(HdfsOrcDataEnvironment.class).in(Scopes.SINGLETON);
    }

    @Singleton
    @Provides
    public Path createBaseLocation(StorageManagerConfig config)
    {
        return new Path(config.getDataDirectory());
    }
}
