import { AxiosRequestConfig } from 'axios';
import { Api } from './generated/Api';
declare const api: (serverUrl: string, token: string, axiosOpts?: AxiosRequestConfig) => Api<unknown>;
export default api;
