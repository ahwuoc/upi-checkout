import type { ClassValue } from 'clsx'
import { clsx } from 'clsx'
import { twMerge } from 'tailwind-merge'

/** `cn()` của shadcn: gộp class Tailwind, class sau thắng khi xung đột. */
export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs))
}
