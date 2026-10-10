import { ClientConfig } from '../../clients/hatchet-client';
import { ChannelCredentials } from 'nice-grpc';
interface LoadClientConfigOptions {
    path?: string;
}
export declare class ConfigLoader {
    static loadClientConfig(override?: Partial<ClientConfig>, config?: LoadClientConfigOptions): Partial<ClientConfig>;
    private static parseIntEnv;
    private static parseFloatEnv;
    private static parseJsonArray;
    static get default_yaml_config_path(): string;
    static createCredentials(config: ClientConfig['tls_config']): ChannelCredentials;
    static loadYamlConfig(path?: string): ClientConfig | undefined;
    private static env;
}
export {};
