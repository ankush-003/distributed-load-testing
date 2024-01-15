import type { Metadata } from 'next'
import { Inter } from 'next/font/google'
import './globals.css'
import { ThemeProvider } from "@/components/theme-provider"
import { cn } from '@/lib/utils'
import Sidebar from '@/components/Sidebar'

const inter = Inter({ subsets: ['latin'] })

export const metadata: Metadata = {
  title: 'Distributed Load Testing',
  description: 'dashboard for distributed load testing',
}

export default function RootLayout({
  children,
}: {
  children: React.ReactNode
}) {
  return (
    <html lang="en">
      <body className={cn(
        "min-h-screen w-full flex",
        inter.className,{
        'debug-screens' : process.env.NODE_ENV === "development"
      })}>
      <ThemeProvider
            attribute="class"
            defaultTheme="dark"
            enableSystem
            disableTransitionOnChange
          >
            {/* Sidebar */}
            <Sidebar />
            {/* Main content */}
            <div className='p-8 w-full'>
            {children}
            </div>
          </ThemeProvider>
      </body>
    </html>
  )
}
