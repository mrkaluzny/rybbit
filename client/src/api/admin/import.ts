import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { authedFetch } from "@/api/utils";
import { APIResponse } from "@/api/types";

interface GetSiteImportsResponse {
  importId: string;
  platform: "umami" | null;
  importedEvents: number;
  skippedEvents: number;
  invalidEvents: number;
  startedAt: string;
  completedAt: string | null;
}

interface CreateSiteImportResponse {
  importId: string;
  allowedDateRange: {
    earliestAllowedDate: string;
    latestAllowedDate: string;
  };
}

interface DeleteImportResponse {
  message: string;
}

export function useGetSiteImports(site: number) {
  return useQuery({
    queryKey: ["get-site-imports", site],
    queryFn: async () => await authedFetch<APIResponse<GetSiteImportsResponse[]>>(`/get-site-imports/${site}`),
    refetchInterval: data => {
      // Check if there are any imports that haven't completed yet (completedAt is null)
      const hasActiveImports = data.state.data?.data.some(imp => imp.completedAt === null);
      return hasActiveImports ? 5000 : false;
    },
    placeholderData: { data: [] },
    staleTime: 30000,
  });
}

export function useCreateSiteImport(site: number) {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: async () => {
      return await authedFetch<APIResponse<CreateSiteImportResponse>>(`/create-site-import/${site}`, undefined, {
        method: "POST",
      });
    },
    onSuccess: async () => {
      await queryClient.invalidateQueries({
        queryKey: ["get-site-imports", site],
      });
    },
    retry: false,
  });
}

export function useDeleteSiteImport(site: number) {
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: async (importId: string) => {
      return await authedFetch<APIResponse<DeleteImportResponse>>(
        `/delete-site-import/${site}/${importId}`,
        undefined,
        {
          method: "DELETE",
        }
      );
    },
    onSuccess: async () => {
      await queryClient.invalidateQueries({
        queryKey: ["get-site-imports", site],
      });
    },
    retry: false,
  });
}
